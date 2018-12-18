package ru.mail.polis.gskoba;

import one.nio.http.*;
import one.nio.net.ConnectionString;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

import static java.lang.Math.abs;

public class PersistentKVService extends HttpServer implements KVService {
    @NotNull
    private final KVDao kvDao;
    private HttpClient me;
    private final String DEFAULT_REPLICAS;
    private final List<HttpClient> nodes = new ArrayList<>();
    private final Logger logger = Logger.getLogger(PersistentKVService.class);
    private final int INTERNAL_ERROR = 500;

    public PersistentKVService(@NotNull HttpServerConfig config, @NotNull KVDao kvDao, @NotNull Set<String> topology) throws IOException {
        super(config);
        this.kvDao = kvDao;
        String port = Integer.toString(config.acceptors[0].port);
        topology.stream().forEach(node -> {
            HttpClient client = new HttpClient(new ConnectionString(node));
            nodes.add(client);
            if (me == null) {
                if (node.split(":")[2].equals(port)) {
                    logger.info("Server on " + node);
                    me = client;
                } else logger.info("Node on " + node);
            } else logger.info("Node on " + node);
        });
        DEFAULT_REPLICAS = (nodes.size() / 2 + 1) + "/" + nodes.size();
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendError(Response.BAD_REQUEST, null);
    }

    @Path("/v0/status")
    public void status(Request request, HttpSession session) throws IOException {
        if (request.getMethod() == Request.METHOD_GET) {
            session.sendResponse(Response.ok(Response.EMPTY));
        } else {
            session.sendError(Response.BAD_REQUEST, null);
        }
    }


    @Path("/v0/entity")
    public void entity(Request request, HttpSession session,
                       @Param("id=") String id, @Param("replicas=") String replicas, @Param("TTL=") long expireTime) throws IOException {
        try {
            if (id == null || id.isEmpty()) {
                session.sendError(Response.BAD_REQUEST, null);
                return;
            }

            final Replicas replicasDef;

            if (replicas == null || replicas.isEmpty()) {
                replicasDef = new Replicas(DEFAULT_REPLICAS, nodes.size());
            } else {
                replicasDef = new Replicas(replicas, nodes.size());
            }

            final boolean isProxied = request.getHeader("proxied") != null;

            logger.info("REQUEST " + replicas + " proxied: " + isProxied);
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    logger.info("METHOD_GET");
                    if (isProxied) {
                        session.sendResponse(get(id));
                    } else {
                        session.sendResponse(proxiedGET(
                                id,
                                replicasDef.getAck(),
                                getNodes(id, replicasDef.getFrom())
                                )
                        );
                    }
                    break;

                case Request.METHOD_PUT:
                    logger.info("METHOD_PUT");
                    if (isProxied) {
                        session.sendResponse(put(id, request.getBody()));
                    } else {
                        session.sendResponse(proxiedPUT(
                                id,
                                request.getBody(),
                                expireTime,
                                replicasDef.getAck(),
                                getNodes(id, replicasDef.getFrom())
                                )
                        );
                    }
                    break;

                case Request.METHOD_DELETE:
                    logger.info("METHOD_DELETE");
                    if (isProxied) {
                        session.sendResponse(delete(id));
                    } else {
                        session.sendResponse(proxiedDELETE(id, replicasDef.getAck(), getNodes(id, replicasDef.getFrom())));
                    }
                    break;

                default:
                    logger.info("METHOD_DEFAULT");
                    session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
            }
        } catch (IllegalArgumentException ex) {
            logger.info(ex);
            session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
        } catch (Exception ex) {
            logger.info(ex);
            session.sendResponse(new Response(Response.INTERNAL_ERROR, Response.EMPTY));
        }
    }

    private Response delete(String id) {
        try {
            Value val = new Value(new byte[0], System.currentTimeMillis(), Value.State.DELETED);
            kvDao.upsert(id.getBytes(), ValueSerializer.INSTANCE.serialize(val));
            logger.info("DELETE id=" + id);
        } catch (IOException ex) {
            logger.info(ex);
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private Response put(String id, byte[] body) {
        try {
            kvDao.upsert(id.getBytes(), ValueSerializer.INSTANCE.serialize(new Value(body)));
            logger.info("PUT id=" + id);
        } catch (IOException ex) {
            logger.info(ex);
        }
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response get(String id) {
        Response response = null;
        try {
            logger.info("GET id=" + id);
            response = new Response(Response.OK, kvDao.get(id.getBytes()));
        } catch (IOException ex) {
            logger.info(ex);
        }
        return response;
    }

    private ArrayList<HttpClient> getNodes(@NotNull String id, final int from) throws IllegalArgumentException {
        if (id.isEmpty()) throw new IllegalArgumentException();
        int position = abs(id.hashCode()) % from;
        ArrayList<HttpClient> result = new ArrayList<>();
        for (int i = 0; i < from; i++) {
            result.add(nodes.get(position));
            position = abs((position + 1)) % from;
        }
        return result;
    }

    private Response proxiedGET(String id, int ack, List<HttpClient> from) {
        List<Value> values = new ArrayList<>(from.size());
        for (HttpClient node : from) {
            if (node == me) {
                try {
                    Value value = ValueSerializer.INSTANCE.deserialize(kvDao.get(id.getBytes()));
                    long currentTime = System.currentTimeMillis();
                    if (currentTime > value.getTTL() && value.getTTL() != null) {
                        proxiedDELETE(id, ack, from);
                    } else {
                        values.add(value);
                    }
                } catch (NoSuchElementException ex) {
                    logger.info(ex);
                    values.add(new Value(new byte[0], Long.MIN_VALUE, Value.State.UNKNOWN));
                } catch (IOException ex) {
                    logger.info(ex);
                }
            } else {
                try {
                    final Response response = node.get("/v0/entity?id=" + id, "proxied: true");
                    values.add(getValue(response));
                } catch (Exception ex) {
                    logger.info(ex);
                }
            }
        }
        if (values.size() >= ack) {
            Value max = values.stream()
                    .max(Comparator.comparingLong(Value::getTimestamp)).get();
            logger.info(max.getState());
            return max.getState() == Value.State.PRESENT ?
                    new Response(Response.OK, max.getData()) :
                    new Response(Response.NOT_FOUND, Response.EMPTY);
        }
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }

    private Value getValue(Response response) {
        return new ValueSerializer().deserialize(response.getBody());
    }

    private Response proxiedPUT(String id, byte[] body, Long expireTime, int ack, List<HttpClient> from) {
        int myAck = 0;
        for (HttpClient node : from) {
            if (node == me) {
                try {
                    if (expireTime != null) {
                        kvDao.upsert(id.getBytes(), ValueSerializer.INSTANCE.serialize(new Value(body, System.currentTimeMillis(), expireTime)));
                    } else {
                        kvDao.upsert(id.getBytes(), ValueSerializer.INSTANCE.serialize(new Value(body, System.currentTimeMillis())));
                    }
                    myAck++;
                } catch (IOException ex) {
                    logger.info(ex);
                }
            } else {
                try {
                    final Response response = node.put("/v0/entity?id=" + id, body, "proxied: true");
                    if (response.getStatus() != INTERNAL_ERROR) {
                        myAck++;
                    }
                } catch (Exception ex) {
                    logger.info(ex);
                }
            }
        }
        if (myAck >= ack) {
            return new Response(Response.CREATED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private Response proxiedDELETE(String id, int ack, List<HttpClient> from) {
        int myAck = 0;
        for (HttpClient node : from) {
            if (node == me) {
                Value val = new Value(new byte[0], System.currentTimeMillis(), Value.State.DELETED);
                try {
                    byte[] ser = ValueSerializer.INSTANCE.serialize(val);
                    kvDao.upsert(id.getBytes(), ser);
                    myAck++;
                } catch (IOException ex) {
                    logger.info(ex);
                }
            } else {
                try {
                    final Response response = node.delete("/v0/entity?id=" + id, "proxied: true");
                    if (response.getStatus() != INTERNAL_ERROR) {
                        myAck++;
                    }
                } catch (Exception ex) {
                    logger.info(ex);
                }
            }
        }
        if (myAck >= ack) {
            return new Response(Response.ACCEPTED, Response.EMPTY);
        }
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }
}

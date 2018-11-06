package ru.mail.polis.gskoba;

import one.nio.http.*;
import one.nio.net.ConnectionString;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;

import java.io.IOException;
import java.util.*;

public class PersistentKVService extends HttpServer implements KVService {

    @NotNull
    private final KVDao kvDao;
    private String[] topology;
    private String me;
    private final String DEFAULT_REPLICAS;
    private final ValueSerializer serializer;
    private final List<HttpClient> nodes = new ArrayList<>();

    public PersistentKVService(@NotNull HttpServerConfig config, @NotNull KVDao kvDao, @NotNull Set<String> topology) throws IOException {
        super(config);
        this.kvDao = kvDao;
        this.topology = topology.toArray(new String[0]);
        me = "http://localhost:" + config.acceptors[0].port;

        System.out.println("ME: " + me);
        topology.stream().forEach(node -> {
            HttpClient client = new HttpClient(new ConnectionString(node));
            nodes.add(client);
        });
        serializer = new ValueSerializer();
        DEFAULT_REPLICAS = nodes.size() + "/" + nodes.size();
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
                           @Param("id=") String id, @Param("replicas=") String replicas) throws IOException {
        try {
            if (id.isEmpty()) {
                session.sendError(Response.BAD_REQUEST, null);
                return;
            }

            Replicas replicasDef;

            if (replicas.isEmpty()) {
                replicasDef = new Replicas(DEFAULT_REPLICAS, nodes.size());
            } else {
                replicasDef = new Replicas(replicas, nodes.size());
            }

            boolean isProxied = request.getHeader("proxied") != null;

            System.out.println("REQUEST " + replicas + " proxied: " + isProxied);

            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    System.out.println("METHOD_GET");
                    if (!isProxied) {
                        session.sendResponse(proxiedGET(id, replicasDef.getAck(), getNodes(id, replicasDef.getFrom())));
                    } else {
                        session.sendResponse(get(id));
                    }
                    break;

                case Request.METHOD_PUT:
                    System.out.println("METHOD_PUT");
                    if (!isProxied) {
                        session.sendResponse(proxiedPUT(id, request.getBody(), replicasDef.getAck(), getNodes(id, replicasDef.getFrom())));
                    } else {
                        session.sendResponse(put(id, request.getBody()));
                    }
                    break;

                case Request.METHOD_DELETE:
                    System.out.println("METHOD_DELETE");
                    if (!isProxied) {
                        session.sendResponse(proxiedDELETE(id, replicasDef.getAck(), getNodes(id, replicasDef.getFrom())));
                    } else {
                        session.sendResponse(delete(id));
                    }
                    break;

                default:
                    session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
            }
        } catch (IllegalArgumentException ex){
            ex.printStackTrace();
            session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
        } catch (Exception ex) {
            ex.printStackTrace();
            session.sendResponse(new Response(Response.INTERNAL_ERROR, Response.EMPTY));
        }
    }

    private Response delete(String id) {
        try {
            Value val = new Value(new byte[0], System.currentTimeMillis(), Value.State.DELETED.ordinal());
            kvDao.upsert(id.getBytes(), serializer.serialize(val));
            System.out.println("DELETE id=" + id);
        } catch (IOException ex) {
            ex.printStackTrace();
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private Response put(String id, byte[] body) {
        try {
            kvDao.upsert(id.getBytes(), serializer.serialize(new Value(body, System.currentTimeMillis())));
            System.out.println("PUT id=" + id);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response get(String id) {
        Response response = null;
        try {
            System.out.println("GET id=" + id);
            Value value = serializer.deserialize(kvDao.get(id.getBytes()));
            response = new Response(Response.OK, value.getData());
            response.addHeader("timestamp" + value.getTimestamp());
            response.addHeader("state" + value.getState().ordinal());
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return response;
    }

    private List<String> getNodes(String key, int length) {
        ArrayList<String> clients = new ArrayList<>();
        int firstNodeId = (key.hashCode() & Integer.MAX_VALUE) % topology.length;
        clients.add(topology[firstNodeId]);
        for (int i = 1; i < length; i++) {
            clients.add(topology[(firstNodeId + i) % topology.length]);
        }
        return clients;
    }

    private Response proxiedGET(String id, int ack, List<String> from) {

        List<Value> values = new ArrayList<>();
        for (String node : from) {

            if (node.equals(me)) {
                try {
                    values.add(serializer.deserialize(kvDao.get(id.getBytes())));
                } catch (NoSuchElementException nSEE) {
                    values.add(new Value(new byte[0], Long.MIN_VALUE, Value.State.UNKNOWN.ordinal()));
                }catch (IOException ex) {
                    ex.printStackTrace();
                }
            } else {
                try {
                    Response response = new HttpClient(new ConnectionString(node)).get("/v0/entity?id=" + id, "proxied: true");
                    values.add(new Value(response.getBody(),
                            Long.parseLong(response.getHeader("timestamp")),
                            Integer.parseInt(response.getHeader("state"))));
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }

        }

        if (values.size() >= ack) {
            Value max = values.stream()
                    .max(Comparator.comparingLong(Value::getTimestamp)).get();
            System.out.println(max.getState());
            return max.getState() == Value.State.PRESENT ?
                    new Response(Response.OK, max.getData()) :
                    new Response(Response.NOT_FOUND, Response.EMPTY);
        }
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);

    }

    private Response proxiedPUT(String id, byte[] body, int ack, List<String> from) {
        int myAck = 0;
        for (String node : from
        ) {
            if (node.equals(me)) {
                try {
                    kvDao.upsert(id.getBytes(), serializer.serialize(new Value(body, System.currentTimeMillis())));
                    myAck++;
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            } else {
                try {
                    Response response = (new HttpClient(new ConnectionString(node))).put("/v0/entity?id=" + id, body, "proxied: true");
                    if (response.getStatus() != 500) {
                        myAck++;
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }

        if (myAck >= ack) {
            return new Response(Response.CREATED, Response.EMPTY);
        }

        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }

    private Response proxiedDELETE(String id, int ack, List<String> from) {
        int myAck = 0;

        for (String node : from) {
            if (node.equals(me)) {
                Value val = new Value(new byte[0], System.currentTimeMillis(), Value.State.DELETED.ordinal());
                try {
                    byte[] ser = serializer.serialize(val);
                    kvDao.upsert(id.getBytes(), ser);
                    myAck++;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    Value val = new Value(new byte[0], System.currentTimeMillis(), Value.State.DELETED.ordinal());
                    byte[] value = serializer.serialize(val);
                    final Response response = new HttpClient(new ConnectionString(node)).delete("/v0/entity?id=" + id, "proxied: true");
                    if (response.getStatus() != 500) {
                        myAck++;
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }

        if (myAck >= ack) {
            return new Response(Response.ACCEPTED, Response.EMPTY);
        }
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }
}

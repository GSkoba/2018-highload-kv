package ru.mail.polis.gskoba;

public class Replicas {
    private int ack, from;

    public Replicas(String replicasIn, int nodesAmount){
        String[] replicas = replicasIn.split("/");
        ack = Integer.parseInt(replicas[0]);
        from = Integer.parseInt(replicas[1]);
        if (ack < 1 || ack > from || from > nodesAmount) throw new IllegalArgumentException();
    }

    public int getAck() {
        return ack;
    }

    public int getFrom() {
        return from;
    }

    @Override
    public String toString() {
        return "Replicas{" +
                "ack=" + ack +
                ", from=" + from +
                '}';
    }
}
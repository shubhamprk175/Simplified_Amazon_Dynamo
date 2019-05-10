package edu.buffalo.cse.cse486586.simpledynamo;




public class SimpleDhtMessage {

    public SimpleDhtMessage(String _msgType, String _srcPort, String _msgData) {
        this.msgType = _msgType;
        this.srcPort = _srcPort;
        this.msgData = _msgData;
        this.destPort = "";
    }

    public SimpleDhtMessage(String _msgType, String _srcPort, String _msgData, String _destPort) {
        this.msgType = _msgType;
        this.srcPort = _srcPort;
        this.msgData = _msgData;
        this.destPort = _destPort;
    }

    public SimpleDhtMessage(){
        this.msgType = "";
        this.srcPort = "";
        this.msgData = "";
        this.destPort = "";
    }

    public String toString() {
        return this.msgType + " : " + this.srcPort + " : "+ this.msgData + " : " + this.destPort;
    }

        public String msgType, srcPort, msgData, destPort;
}

package edu.buffalo.cse.cse486586.simpledynamo;




public class SimpleDhtMessage {

        public SimpleDhtMessage(String type, String mPort, String data) {
            this.msgType = type;
            this.srcPort = mPort;
            this.msgData = data;
        }

        public String msgType, srcPort, msgData;
}


import io.filemq.jms.FileMQConnectionFactory;
import io.filemq.jms.FileMQQueue;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

public class Driver {

  public static void main(String[] args) throws Throwable {
    ConnectionFactory cf = new FileMQConnectionFactory();
    Destination dest = new FileMQQueue("TEST.FOO");

    try (Connection conn = cf.createConnection();
            Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);) {

      /*
      MessageConsumer consumer = sess.createConsumer(dest);
      consumer.setMessageListener(new MessageListener() {
        @Override
        public void onMessage(Message message) {
          try {
            System.out.println(String.format("Got a message JMSMessageID: [%s], MyAwesomeHeader: [%s], Body: [%s]", message.getJMSMessageID(), message.getObjectProperty("MyAwesomeHeader"), ((TextMessage) message).getText()));
          } catch (JMSException e) {
            e.printStackTrace();
          }
        }
      });
       */
      conn.start();

      MessageProducer producer = sess.createProducer(dest);
      Message message = sess.createTextMessage("Hello world!");
      message.setBooleanProperty("MyAwesomeHeader", true);
      producer.send(message);
      Message message2 = sess.createTextMessage("Hello world 2!");
      message2.setBooleanProperty("MyAwesomeHeader", false);
      producer.send(message2);
    }
  }
}

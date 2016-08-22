/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.filemq.jms.test;

import io.filemq.jms.FileMQConnectionFactory;
import io.filemq.jms.FileMQQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class MessageConsumerTest {

  private static ConnectionFactory connectionFactory;
  private static Destination destination;

  private Connection connection;
  private Session session;
  private MessageConsumer consumer;

  public MessageConsumerTest() {
  }

  @BeforeClass
  public static void initClass() {
    connectionFactory = new FileMQConnectionFactory();
    destination = new FileMQQueue("TEST.FOO");
  }

  @Before
  public void initTest() throws JMSException {
    connection = connectionFactory.createConnection();
    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    consumer = session.createConsumer(destination);
  }

  @Test
  public void testConsumer_1000x1k() throws Exception {
    long start = System.currentTimeMillis();

    final AtomicBoolean running = new AtomicBoolean();
    final AtomicInteger i = new AtomicInteger(0);
    consumer.setMessageListener(new MessageListener() {
      @Override
      public void onMessage(Message message) {
        if (i.get() >= 1000) {
          try { connection.stop(); } catch (JMSException e) {}
          running.set(false);
          System.out.println(String.format("Test took [%s] millis.", System.currentTimeMillis() - start));
        }
        i.getAndIncrement();
      }
    });
    running.set(true);
    connection.start();
    
    while (running.get()) {
      try { Thread.sleep(5000L); } catch (InterruptedException e) {}
    }
  }
}

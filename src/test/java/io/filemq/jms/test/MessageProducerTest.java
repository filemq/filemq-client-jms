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
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class MessageProducerTest {
  
  private static ConnectionFactory connectionFactory;
  private static Destination destination;
  
  private Connection connection;
  private Session session;
  private MessageProducer producer;
  
  public MessageProducerTest() {
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
    producer = session.createProducer(destination);
  }
  
  @Test
  public void testProducer_1000x1k() throws Exception {
    long start = System.currentTimeMillis();
    
    for (int i = 0; i < 1000; ++i) {
      Message msg = session.createTextMessage("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Integer sed bibendum nibh, in tempus elit. Ut vitae augue finibus, rutrum erat id, luctus arcu. Vivamus vitae ipsum arcu. Cras nec luctus justo. Suspendisse venenatis convallis ante, vulputate euismod mi feugiat a. In eu enim nec dui euismod malesuada. Aliquam sed ipsum non nunc congue sagittis. Morbi est nulla, iaculis et tincidunt facilisis, porttitor eget quam. Sed mauris neque, efficitur tempus turpis maximus, ornare fringilla mauris. Phasellus id sem lacus. Mauris et risus est. Cras pulvinar tincidunt hendrerit. Integer mollis rutrum enim, ut luctus tellus volutpat non. Etiam sagittis libero quis ex sollicitudin, sit amet porttitor est viverra. Sed pretium id nisl quis egestas. Cras venenatis lacus sit amet volutpat varius. Nam blandit risus eu vehicula porttitor. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Phasellus a cursus mauris. Sed ut scelerisque velit. Etiam commodo lacus ex, ut facilisis metus.");
      msg.setObjectProperty("UserHeader01", "foo");
      producer.send(msg);
    }
    
    System.out.println(String.format("Test took [%s] millis.", System.currentTimeMillis() - start));
  }
  
  @Test
  public void testProducer_1000x100b() throws Exception {
    long start = System.currentTimeMillis();
    
    for (int i = 0; i < 1000; ++i) {
      Message msg = session.createTextMessage("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Integer neque nulla, consequat non posuere.");
      msg.setObjectProperty("UserHeader01", "foo");
      producer.send(msg);
    }
    
    System.out.println(String.format("Test took [%s] millis.", System.currentTimeMillis() - start));
  }
}

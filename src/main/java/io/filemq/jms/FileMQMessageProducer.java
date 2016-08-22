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
package io.filemq.jms;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.UserDefinedFileAttributeView;
import java.util.Enumeration;
import java.util.UUID;
import javax.jms.CompletionListener;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

class FileMQMessageProducer implements MessageProducer {

  private final Session session;
  private final FileMQDestination defaultDestination;
  
  public FileMQMessageProducer(Session session) {
    this(session, null);
  }

  public FileMQMessageProducer(Session session, FileMQDestination defaultDestination) {
    this.session = session;
    this.defaultDestination = defaultDestination;
  }
  
  @Override
  public void setDisableMessageID(boolean value) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public boolean getDisableMessageID() throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void setDisableMessageTimestamp(boolean value) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public boolean getDisableMessageTimestamp() throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void setDeliveryMode(int deliveryMode) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public int getDeliveryMode() throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void setPriority(int defaultPriority) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public int getPriority() throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void setTimeToLive(long timeToLive) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public long getTimeToLive() throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void setDeliveryDelay(long deliveryDelay) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public long getDeliveryDelay() throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Destination getDestination() throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void close() throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void send(Message message) throws JMSException {
    message.setJMSMessageID("ID:" + UUID.randomUUID().toString());
    try {
      Path file = Paths.get("/home/jreagan/Downloads", defaultDestination.getName(), message.getJMSMessageID() + ".msg");
      Path tempFile = file.getParent().resolve(".tmp").resolve(message.getJMSMessageID() + ".msg");

      Files.write(tempFile, message.getBody(String.class).getBytes("UTF-8"), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.SYNC);

      UserDefinedFileAttributeView view = Files.getFileAttributeView(tempFile, UserDefinedFileAttributeView.class);
      view.write("broker." + "JMSMessageID", Charset.forName("UTF-8").encode(message.getJMSMessageID()));
      
      Enumeration propertyNames = message.getPropertyNames();
      while(propertyNames.hasMoreElements()) {
        String propertyName = (String) propertyNames.nextElement();
        view.write("broker." + propertyName, Charset.forName("UTF-8").encode(message.getObjectProperty(propertyName).toString()));
      }

      Files.move(tempFile, file, StandardCopyOption.ATOMIC_MOVE);
    } catch (IOException e) {
      throw new JMSException(e.toString());
    }
  }

  @Override
  public void send(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void send(Destination destination, Message message) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void send(Message message, CompletionListener completionListener) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void send(Message message, int deliveryMode, int priority, long timeToLive, CompletionListener completionListener) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void send(Destination destination, Message message, CompletionListener completionListener) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive, CompletionListener completionListener) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }
}

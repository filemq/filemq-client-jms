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

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import static io.filemq.jms.FileMQHelper.*;

class FileMQSession implements Session {

  
  private final ObservableBoolean closed = new ObservableBoolean(false);
  private final ReentrantReadWriteLock closedLock = new ReentrantReadWriteLock();
  
  private final FileMQConnection connection;

  private final UUID sessionID;
  
  private UUID currentTransactionID;

  public FileMQSession(FileMQConnection connection) {
    this.connection = connection;
    
    this.sessionID = UUID.randomUUID();
    this.currentTransactionID = UUID.randomUUID();
  }

  public FileMQConnection getConnection() {
    return connection;
  }

  public UUID getSessionID() {
    return sessionID;
  }

  public UUID getCurrentTransactionID() {
    return currentTransactionID;
  }
  
  @Override
  public BytesMessage createBytesMessage() throws JMSException {
    return new FileMQBytesMessage();
  }

  @Override
  public MapMessage createMapMessage() throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Message createMessage() throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public ObjectMessage createObjectMessage() throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public ObjectMessage createObjectMessage(Serializable object) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public StreamMessage createStreamMessage() throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public TextMessage createTextMessage() throws JMSException {
    return createTextMessage(null);
  }

  @Override
  public TextMessage createTextMessage(String text) throws JMSException {
    FileMQTextMessage message = new FileMQTextMessage();
    message.setText(text);
    return message;
  }

  @Override
  public boolean getTransacted() throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public int getAcknowledgeMode() throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void commit() throws JMSException {
    currentTransactionID = UUID.randomUUID();
  }

  @Override
  public void rollback() throws JMSException {
    currentTransactionID = UUID.randomUUID();
  }

  @Override
  public void close() throws JMSException {
    closedLock.writeLock().lock();
    try {
      if (!closed.get()) {
        unwrap(closed::set, true);
      }
    } finally {
      closedLock.writeLock().unlock();
    }
  }

  @Override
  public void recover() throws JMSException {
    currentTransactionID = UUID.randomUUID();
  }

  @Override
  public MessageListener getMessageListener() throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void setMessageListener(MessageListener listener) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void run() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public MessageProducer createProducer(Destination destination) throws JMSException {
    return new FileMQMessageProducer(this, (FileMQDestination) destination);
  }

  @Override
  public MessageConsumer createConsumer(Destination destination) throws JMSException {
    return new FileMQMessageConsumer(this, destination);
  }

  @Override
  public MessageConsumer createConsumer(Destination destination, String messageSelector) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public MessageConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public MessageConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public MessageConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName, String messageSelector) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Queue createQueue(String queueName) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Topic createTopic(String topicName) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public TopicSubscriber createDurableSubscriber(Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public MessageConsumer createDurableConsumer(Topic topic, String name) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public MessageConsumer createDurableConsumer(Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public MessageConsumer createSharedDurableConsumer(Topic topic, String name) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public MessageConsumer createSharedDurableConsumer(Topic topic, String name, String messageSelector) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public QueueBrowser createBrowser(Queue queue) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public TemporaryQueue createTemporaryQueue() throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public TemporaryTopic createTemporaryTopic() throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void unsubscribe(String name) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }
}

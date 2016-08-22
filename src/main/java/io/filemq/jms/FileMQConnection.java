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

import java.nio.file.Path;
import java.util.Deque;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;

import static io.filemq.jms.FileMQHelper.*;

class FileMQConnection implements Connection {

  private final Path workDirectory;
  private final Path persistentDirectory;
  private final Path nonPersistentDirectory;

  private final ObservableBoolean started = new ObservableBoolean(false);
  private final ReentrantReadWriteLock startedLock = new ReentrantReadWriteLock();

  private final ObservableBoolean closed = new ObservableBoolean(false);
  private final ReentrantReadWriteLock closedLock = new ReentrantReadWriteLock();

  private final UUID connectionID;

  private String clientID;
  private ExceptionListener exceptionListener;

  private final Map<UUID, FileMQSession> sessions = new ConcurrentHashMap<>();

  private final Map<UUID, FileMQMessageConsumer> messageConsumers = new ConcurrentHashMap<>();

  public FileMQConnection(Path workDirectory, Path persistentDirectory, Path nonPersistentDirectory) {
    this.workDirectory = workDirectory;
    this.persistentDirectory = persistentDirectory;
    this.nonPersistentDirectory = nonPersistentDirectory;

    this.connectionID = UUID.randomUUID();
  }

  public Path getWorkDirectory() {
    return workDirectory;
  }

  public Path getPersistentDirectory() {
    return persistentDirectory;
  }

  public Path getNonPersistentDirectory() {
    return nonPersistentDirectory;
  }

  public UUID getConnectionID() {
    return connectionID;
  }

  public Deque<Path> registerMessageConsumer(FileMQMessageConsumer messageConsumer) {
    return null;
  }

  @Override
  public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {
    closedLock.readLock().lock();
    try {
      if (closed.get()) {
        throw new IllegalStateException(String.format("Connection [%s] is closed.", connectionID));
      }
      FileMQSession session = new FileMQSession(this);
      closed.addObserver(new AutoCloseableObserver(session));
      sessions.put(session.getSessionID(), session);
      return session;
    } finally {
      closedLock.readLock().unlock();
    }
  }

  @Override
  public Session createSession(int sessionMode) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Session createSession() throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public String getClientID() throws JMSException {
    closedLock.readLock().lock();
    try {
      if (closed.get()) {
        throw new IllegalStateException(String.format("Connection [%s] is closed.", connectionID));
      }
      return clientID;
    } finally {
      closedLock.readLock().unlock();
    }
  }

  @Override
  public void setClientID(String clientID) throws JMSException {
    closedLock.readLock().lock();
    try {
      if (closed.get()) {
        throw new IllegalStateException(String.format("Connection [%s] is closed.", connectionID));
      }
      this.clientID = clientID;
    } finally {
      closedLock.readLock().unlock();
    }
  }

  @Override
  public ConnectionMetaData getMetaData() throws JMSException {
    closedLock.readLock().lock();
    try {
      if (closed.get()) {
        throw new IllegalStateException(String.format("Connection [%s] is closed.", connectionID));
      }
      // TODO: Implement this method.
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    } finally {
      closedLock.readLock().unlock();
    }
  }

  @Override
  public ExceptionListener getExceptionListener() throws JMSException {
    closedLock.readLock().lock();
    try {
      if (closed.get()) {
        throw new IllegalStateException(String.format("Connection [%s] is closed.", connectionID));
      }
      return exceptionListener;
    } finally {
      closedLock.readLock().unlock();
    }
  }

  @Override
  public void setExceptionListener(ExceptionListener listener) throws JMSException {
    closedLock.readLock().lock();
    try {
      if (closed.get()) {
        throw new IllegalStateException(String.format("Connection [%s] is closed.", connectionID));
      }
      this.exceptionListener = listener;
    } finally {
      closedLock.readLock().unlock();
    }
  }

  @Override
  public void start() throws JMSException {
    closedLock.readLock().lock();
    try {
      if (closed.get()) {
        throw new IllegalStateException(String.format("Connection [%s] is closed.", connectionID));
      }
      startedLock.writeLock().lock();
      try {
        if (!started.get()) {
          // Start the file watchers
          for (Map.Entry<UUID, FileMQMessageConsumer> messageConsumerEntry : messageConsumers.entrySet()) {
            messageConsumerEntry.getValue().start();
          }
          started.set(true);
        }
      } finally {
        startedLock.writeLock().unlock();
      }
    } finally {
      closedLock.readLock().unlock();
    }
  }

  @Override
  public void stop() throws JMSException {
    closedLock.readLock().lock();
    try {
      if (closed.get()) {
        throw new IllegalStateException(String.format("Connection [%s] is closed.", connectionID));
      }
      startedLock.writeLock().lock();
      try {
        if (started.get()) {
          // Stop the file watchers
          for (Map.Entry<UUID, FileMQMessageConsumer> messageConsumerEntry : messageConsumers.entrySet()) {
            messageConsumerEntry.getValue().stop();
          }
          started.set(false);
        }
      } finally {
        startedLock.writeLock().unlock();
      }
    } finally {
      closedLock.readLock().unlock();
    }
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
  public ConnectionConsumer createConnectionConsumer(Destination destination, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public ConnectionConsumer createSharedConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public ConnectionConsumer createSharedDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }
}

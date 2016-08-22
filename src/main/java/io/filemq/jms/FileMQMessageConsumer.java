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
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserDefinedFileAttributeView;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;

class FileMQMessageConsumer implements MessageConsumer {

  private final FileMQSession session;
  private final Destination destination;

  private boolean running = false;
  private Path dir;
  private WatchService watchService;
  private Executor pool;

  private MessageListener listener;

  public FileMQMessageConsumer(FileMQSession session, Destination destination) {
    this.session = session;
    this.destination = destination;
  }

  public FileMQSession getSession() {
    return session;
  }

  public Destination getDestination() {
    return destination;
  }

  @Override
  public String getMessageSelector() throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public MessageListener getMessageListener() throws JMSException {
    return listener;
  }

  @Override
  public void setMessageListener(MessageListener listener) throws JMSException {
    this.listener = listener;
    if (listener != null) {
      //session.getConnection().getMessageConsumers().add(this);
    }
  }

  @Override
  public Message receive() throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Message receive(long timeout) throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Message receiveNoWait() throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void close() throws JMSException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  public void start() throws JMSException {
    if (!running) {
      running = true;

      pool = Executors.newSingleThreadExecutor();
      pool.execute(new Runnable() {
        @Override
        public void run() {

          try {
            dir = Paths.get("/home/jreagan/Downloads", destination.toString());

            final AtomicBoolean processed = new AtomicBoolean();
            while (running) {
              do {
                processed.set(false);
                try (Stream<Path> paths = Files.list(dir)) {
                  paths.forEachOrdered(new Consumer<Path>() {
                    @Override
                    public void accept(Path file) {
                      if (Files.isRegularFile(file)) {
                        processed.set(true);
                        try {
                          FileMQTextMessage message = new FileMQTextMessage();

                          Map<String, Object> properties = new HashMap<>();
                          UserDefinedFileAttributeView view = Files.getFileAttributeView(file, UserDefinedFileAttributeView.class);
                          for (String attributeName : view.list()) {
                            if (attributeName.startsWith("broker.JMSX")) {

                            } else if (attributeName.startsWith("broker.JMS")) {
                              ByteBuffer buf = ByteBuffer.allocate(view.size(attributeName));
                              view.read(attributeName, buf);
                              buf.flip();
                              String attributeValue = Charset.forName("UTF-8").decode(buf).toString();
                              message.setJMSMessageID(attributeValue);
                            } else if (attributeName.startsWith("broker.")) {
                              ByteBuffer buf = ByteBuffer.allocate(view.size(attributeName));
                              view.read(attributeName, buf);
                              buf.flip();
                              String attributeValue = Charset.forName("UTF-8").decode(buf).toString();
                              message.setObjectProperty(attributeName.substring("broker.".length()), attributeValue);
                            }
                          }

                          message.setText(new String(Files.readAllBytes(file), Charset.forName("UTF-8")));

                          listener.onMessage(message);

                          Files.delete(file);
                        } catch (IOException | JMSException e) {
                          throw new RuntimeException(e.toString());
                        }
                      }
                    }
                  });
                }
              } while (processed.get());
            }

            watchService = dir.getFileSystem().newWatchService();

            dir.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
            while (running) {
              WatchKey watchKey = null;
              try {
                watchKey = watchService.take();
              } catch (InterruptedException e) {
                continue;
              }

              for (WatchEvent<?> watchEvent : watchKey.pollEvents()) {
                if (watchEvent.kind() == StandardWatchEventKinds.ENTRY_CREATE) {
                  Path filename = ((WatchEvent<Path>) watchEvent).context();
                  Path file = dir.resolve(filename);

                  FileMQTextMessage message = new FileMQTextMessage();

                  Map<String, Object> properties = new HashMap<>();
                  UserDefinedFileAttributeView view = Files.getFileAttributeView(file, UserDefinedFileAttributeView.class);
                  for (String attributeName : view.list()) {
                    if (attributeName.startsWith("broker.JMSX")) {

                    } else if (attributeName.startsWith("broker.JMS")) {
                      ByteBuffer buf = ByteBuffer.allocate(view.size(attributeName));
                      view.read(attributeName, buf);
                      buf.flip();
                      String attributeValue = Charset.forName("UTF-8").decode(buf).toString();
                      message.setJMSMessageID(attributeValue);
                    } else if (attributeName.startsWith("broker.")) {
                      ByteBuffer buf = ByteBuffer.allocate(view.size(attributeName));
                      view.read(attributeName, buf);
                      buf.flip();
                      String attributeValue = Charset.forName("UTF-8").decode(buf).toString();
                      message.setObjectProperty(attributeName.substring("broker.".length()), attributeValue);
                    }
                  }

                  message.setText(new String(Files.readAllBytes(file), Charset.forName("UTF-8")));

                  listener.onMessage(message);

                  Files.delete(file);

                  if (!watchKey.reset()) {
                    break;
                  }
                }
              }
            }
          } catch (IOException | JMSException e) {
            throw new RuntimeException(e.toString());
          }
        }
      });
    }
  }

  public void stop() throws JMSException {
    running = false;
  }
}

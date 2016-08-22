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
import java.nio.file.Path;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;

public class FileMQConnectionFactory implements ConnectionFactory, Serializable {

  private Path workDirectory;
  private Path persistentDirectory;
  private Path nonPersistentDirectory;

  public Path getWorkDirectory() {
    return workDirectory;
  }

  public void setWorkDirectory(Path workDirectory) {
    this.workDirectory = workDirectory;
  }

  public Path getPersistentDirectory() {
    return persistentDirectory;
  }

  public void setPersistentDirectory(Path persistentDirectory) {
    this.persistentDirectory = persistentDirectory;
  }

  public Path getNonPersistentDirectory() {
    return nonPersistentDirectory;
  }

  public void setNonPersistentDirectory(Path nonPersistentDirectory) {
    this.nonPersistentDirectory = nonPersistentDirectory;
  }
  
  @Override
  public Connection createConnection() throws JMSException {
    return createConnection(null, null);
  }

  @Override
  public Connection createConnection(String userName, String password) throws JMSException {
    FileMQConnection connection = new FileMQConnection(workDirectory, persistentDirectory, nonPersistentDirectory);
    return connection;
  }

  @Override
  public JMSContext createContext() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public JMSContext createContext(String userName, String password) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public JMSContext createContext(String userName, String password, int sessionMode) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public JMSContext createContext(int sessionMode) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }
}

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

import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.jms.JMSException;

public final class FileMQHelper {
  
  public static JMSException newJMSException(Exception cause) {
   return newJMSException(null, null, cause);
  }
  
  public static JMSException newJMSException(String reason, Exception cause) {
   return newJMSException(reason, null, cause);
  }
  
  public static JMSException newJMSException(String reason, String errorCode, Exception cause) {
    JMSException e = new JMSException(reason, errorCode);
    e.setLinkedException(cause);
    return e;
  }

  public static <T> T wrap(Supplier_WithException<T> f, Class<T> c) {
    try {
      return f.get();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new WrappedException(e);
    }
  }

  public static <T> void wrap(Consumer_WithException<T> f, T t) {
    try {
      f.accept(t);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new WrappedException(e);
    }
  }

  public static void wrap(Runnable_WithException f) {
    try {
      f.run();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new WrappedException(e);
    }
  }

  public static <T> T unwrap(Supplier<T> f) throws JMSException {
    try {
      try {
        return f.get();
      } catch (WrappedException e) {
        throw e.unwrap();
      }
    } catch (RuntimeException | JMSException e) {
      throw e;
    } catch (Exception e) {
      JMSException jmsEx = new JMSException(e.getMessage());
      jmsEx.setLinkedException(e);
      throw jmsEx;
    }
  }

  public static <T> void unwrap(Consumer<T> f, T t) throws JMSException {
    try {
      try {
        f.accept(t);
      } catch (WrappedException e) {
        throw e.unwrap();
      }
    } catch (RuntimeException | JMSException e) {
      throw e;
    } catch (Exception e) {
      JMSException jmsEx = new JMSException(e.getMessage());
      jmsEx.setLinkedException(e);
      throw jmsEx;
    }
  }

  @FunctionalInterface
  interface Supplier_WithException<T> {

    public T get() throws Exception;
  }

  @FunctionalInterface
  interface Consumer_WithException<T> {

    public void accept(T t) throws Exception;
  }

  @FunctionalInterface
  interface Runnable_WithException {

    public void run() throws Exception;
  }
}

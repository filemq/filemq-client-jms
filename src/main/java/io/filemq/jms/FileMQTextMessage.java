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

import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import javax.jms.TextMessage;

public class FileMQTextMessage extends AbstractFileMQMessage implements TextMessage {

  private String body;
  
  @Override
  public void setText(String string) throws JMSException {
    if (readOnlyMode) {
      throw new MessageNotWriteableException("Message is in read only mode.");
    }
    
    this.body = string;
  }

  @Override
  public String getText() throws JMSException {
    return body;
  }

  @Override
  public void clearBody() throws JMSException {
    this.body = null;
    
    readOnlyMode = false;
  }

  @Override
  public <T> T getBody(Class<T> c) throws JMSException {
    if (body == null) {
      return null;
    }
    
    if (isBodyAssignableTo(c)) {
      return c.cast(body);
    } else {
      throw new MessageFormatException(String.format("Unable to convert the message body to type [%s].", c));
    }
  }
  
  @Override
  public boolean isBodyAssignableTo(Class c) throws JMSException {
    if (body == null) {
      return true;
    }
    
    return (c != null && String.class.isAssignableFrom(c));
  }
}

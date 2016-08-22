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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import javax.jms.BytesMessage;
import javax.jms.JMSException;

import static io.filemq.jms.FileMQHelper.*;
import java.io.DataOutputStream;
import java.util.Objects;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;

public class FileMQBytesMessage extends AbstractFileMQMessage implements BytesMessage {

  private byte[] body;

  private ByteArrayInputStream bin;
  private DataInputStream din;

  private ByteArrayOutputStream bout = new ByteArrayOutputStream();
  private DataOutputStream dout = new DataOutputStream(bout);

  @Override
  public long getBodyLength() throws JMSException {
    if (!readOnlyMode) {
      throw new MessageNotReadableException("Message is in write only mode.");
    }

    return body.length;
  }

  @Override
  public boolean readBoolean() throws JMSException {
    if (!readOnlyMode) {
      throw new MessageNotReadableException("Message is in write only mode.");
    }

    try {
      return din.readBoolean();
    } catch (IOException e) {
      throw newJMSException("Unable to read boolean value.", e);
    }
  }

  @Override
  public byte readByte() throws JMSException {
    if (!readOnlyMode) {
      throw new MessageNotReadableException("Message is in write only mode.");
    }

    try {
      return din.readByte();
    } catch (IOException e) {
      throw newJMSException("Unable to read byte value.", e);
    }
  }

  @Override
  public int readUnsignedByte() throws JMSException {
    if (!readOnlyMode) {
      throw new MessageNotReadableException("Message is in write only mode.");
    }

    try {
      return din.readUnsignedByte();
    } catch (IOException e) {
      throw newJMSException("Unable to read unsigned byte value.", e);
    }
  }

  @Override
  public short readShort() throws JMSException {
    if (!readOnlyMode) {
      throw new MessageNotReadableException("Message is in write only mode.");
    }

    try {
      return din.readShort();
    } catch (IOException e) {
      throw newJMSException("Unable to read short value.", e);
    }
  }

  @Override
  public int readUnsignedShort() throws JMSException {
    if (!readOnlyMode) {
      throw new MessageNotReadableException("Message is in write only mode.");
    }

    try {
      return din.readUnsignedShort();
    } catch (IOException e) {
      throw newJMSException("Unable to read unsigned short value.", e);
    }
  }

  @Override
  public char readChar() throws JMSException {
    if (!readOnlyMode) {
      throw new MessageNotReadableException("Message is in write only mode.");
    }

    try {
      return din.readChar();
    } catch (IOException e) {
      throw newJMSException("Unable to read char value.", e);
    }
  }

  @Override
  public int readInt() throws JMSException {
    if (!readOnlyMode) {
      throw new MessageNotReadableException("Message is in write only mode.");
    }

    try {
      return din.readInt();
    } catch (IOException e) {
      throw newJMSException("Unable to read int value.", e);
    }
  }

  @Override
  public long readLong() throws JMSException {
    if (!readOnlyMode) {
      throw new MessageNotReadableException("Message is in write only mode.");
    }

    try {
      return din.readLong();
    } catch (IOException e) {
      throw newJMSException("Unable to read long value.", e);
    }
  }

  @Override
  public float readFloat() throws JMSException {
    if (!readOnlyMode) {
      throw new MessageNotReadableException("Message is in write only mode.");
    }

    try {
      return din.readFloat();
    } catch (IOException e) {
      throw newJMSException("Unable to read float value.", e);
    }
  }

  @Override
  public double readDouble() throws JMSException {
    if (!readOnlyMode) {
      throw new MessageNotReadableException("Message is in write only mode.");
    }

    try {
      return din.readDouble();
    } catch (IOException e) {
      throw newJMSException("Unable to read double value.", e);
    }
  }

  @Override
  public String readUTF() throws JMSException {
    if (!readOnlyMode) {
      throw new MessageNotReadableException("Message is in write only mode.");
    }

    try {
      return din.readUTF();
    } catch (IOException e) {
      throw newJMSException("Unable to read UTF value.", e);
    }
  }

  @Override
  public int readBytes(byte[] value) throws JMSException {
    Objects.requireNonNull(value, "Cannot read into a null byte array.");
    if (!readOnlyMode) {
      throw new MessageNotReadableException("Message is in write only mode.");
    }

    try {
      return din.read(value);
    } catch (IOException e) {
      throw newJMSException("Unable to read bytes.", e);
    }
  }

  @Override
  public int readBytes(byte[] value, int length) throws JMSException {
    Objects.requireNonNull(value, "Cannot read into a null byte array.");
    if (!readOnlyMode) {
      throw new MessageNotReadableException("Message is in write only mode.");
    }

    try {
      return din.read(value, 0, length);
    } catch (IOException e) {
      throw newJMSException("Unable to read bytes.", e);
    }
  }

  @Override
  public void writeBoolean(boolean value) throws JMSException {
    if (readOnlyMode) {
      throw new MessageNotWriteableException("Message is in read only mode.");
    }

    try {
      dout.writeBoolean(value);
    } catch (IOException e) {
      throw newJMSException("Unable to write boolean.", e);
    }
  }

  @Override
  public void writeByte(byte value) throws JMSException {
    if (readOnlyMode) {
      throw new MessageNotWriteableException("Message is in read only mode.");
    }

    try {
      dout.writeByte(value);
    } catch (IOException e) {
      throw newJMSException("Unable to write byte.", e);
    }
  }

  @Override
  public void writeShort(short value) throws JMSException {
    if (readOnlyMode) {
      throw new MessageNotWriteableException("Message is in read only mode.");
    }

    try {
      dout.writeShort(value);
    } catch (IOException e) {
      throw newJMSException("Unable to write short.", e);
    }
  }

  @Override
  public void writeChar(char value) throws JMSException {
    if (readOnlyMode) {
      throw new MessageNotWriteableException("Message is in read only mode.");
    }

    try {
      dout.writeChar(value);
    } catch (IOException e) {
      throw newJMSException("Unable to write char.", e);
    }
  }

  @Override
  public void writeInt(int value) throws JMSException {
    if (readOnlyMode) {
      throw new MessageNotWriteableException("Message is in read only mode.");
    }

    try {
      dout.writeInt(value);
    } catch (IOException e) {
      throw newJMSException("Unable to write int.", e);
    }
  }

  @Override
  public void writeLong(long value) throws JMSException {
    if (readOnlyMode) {
      throw new MessageNotWriteableException("Message is in read only mode.");
    }

    try {
      dout.writeLong(value);
    } catch (IOException e) {
      throw newJMSException("Unable to write long.", e);
    }
  }

  @Override
  public void writeFloat(float value) throws JMSException {
    if (readOnlyMode) {
      throw new MessageNotWriteableException("Message is in read only mode.");
    }

    try {
      dout.writeFloat(value);
    } catch (IOException e) {
      throw newJMSException("Unable to write float.", e);
    }
  }

  @Override
  public void writeDouble(double value) throws JMSException {
    if (readOnlyMode) {
      throw new MessageNotWriteableException("Message is in read only mode.");
    }

    try {
      dout.writeDouble(value);
    } catch (IOException e) {
      throw newJMSException("Unable to write double.", e);
    }
  }

  @Override
  public void writeUTF(String value) throws JMSException {
    Objects.requireNonNull(value, "Cannot write null string.");
    if (readOnlyMode) {
      throw new MessageNotWriteableException("Message is in read only mode.");
    }

    try {
      dout.writeUTF(value);
    } catch (IOException e) {
      throw newJMSException("Unable to write UTF.", e);
    }
  }

  @Override
  public void writeBytes(byte[] value) throws JMSException {
    Objects.requireNonNull(value, "Cannot write a null byte array.");
    if (readOnlyMode) {
      throw new MessageNotWriteableException("Message is in read only mode.");
    }

    try {
      dout.write(value);
    } catch (IOException e) {
      throw newJMSException("Unable to write bytes.", e);
    }
  }

  @Override
  public void writeBytes(byte[] value, int offset, int length) throws JMSException {
    Objects.requireNonNull(value, "Cannot write a null byte array.");
    if (readOnlyMode) {
      throw new MessageNotWriteableException("Message is in read only mode.");
    }

    try {
      dout.write(value, offset, length);
    } catch (IOException e) {
      throw newJMSException("Unable to write bytes.", e);
    }
  }

  @Override
  public void writeObject(Object value) throws JMSException {
    Objects.requireNonNull(value, "Cannot write a null object.");

    if (value instanceof Byte) {
      writeByte(((Byte) value));
    } else if (value instanceof Character) {
      writeChar((Character) value);
    } else if (value instanceof Short) {
      writeShort((Short) value);
    } else if (value instanceof Integer) {
      writeInt((Integer) value);
    } else if (value instanceof Long) {
      writeLong((Long) value);
    } else if (value instanceof Float) {
      writeFloat((Float) value);
    } else if (value instanceof Double) {
      writeDouble((Double) value);
    } else if (value instanceof String) {
      writeUTF((String) value);
    } else if (value instanceof byte[]) {
      writeBytes((byte[]) value);
    } else {
      throw new MessageFormatException(String.format("Unable to write object of type [%s].", value.getClass()));
    }
  }

  @Override
  public void reset() throws JMSException {
    if (!readOnlyMode) {
      readOnlyMode = true;
      body = bout.toByteArray();
      try {
        dout.close();
        bout.close();
      } catch (IOException e) {
        throw newJMSException("Unable to close output streams.", e);
      }
      bin = new ByteArrayInputStream(body);
      din = new DataInputStream(bin);
    } else {
      try {
        din.reset();
      } catch (IOException e) {
        throw newJMSException("Unable to reset input streams.", e);
      }
    }
  }

  @Override
  public void clearBody() throws JMSException {
    if (readOnlyMode) {
      readOnlyMode = false;
      body = null;
      try {
        din.close();
        bin.close();
      } catch (IOException e) {
        throw newJMSException("Unable to close input streams.", e);
      }
      bout = new ByteArrayOutputStream();
      dout = new DataOutputStream(bout);
    } else {
      body = null;
      try {
        dout.close();
        bout.close();
      } catch (IOException e) {
        throw newJMSException("Unable to close output streams.", e);
      }
      bout = new ByteArrayOutputStream();
      dout = new DataOutputStream(bout);
    }
  }

  @Override
  public <T> T getBody(Class<T> c) throws JMSException {
    if (!readOnlyMode) {
      throw new MessageNotReadableException("Message is in write only mode.");
    }

    if (body == null || body.length == 0) {
      return null;
    }

    try {
      if (isBodyAssignableTo(c)) {
        return c.cast(body);
      } else {
        throw new MessageFormatException(String.format("Unable to convert the message body to type [%s].", c));
      }
    } finally {
      reset();
    }
  }

  @Override
  public boolean isBodyAssignableTo(Class c) throws JMSException {
    if (body == null || body.length == 0) {
      return true;
    }

    return (c == byte[].class || c == Object.class);
  }
}

package edu.fiu.cs.yxjiang.event.stat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MessageKey implements WritableComparable<MessageKey> {

  private long timestamp;
  private String messageSource; // the IP of the machine who generate the
                                // message

  public MessageKey(long timestamp, String messageSource) {
    this.timestamp = timestamp;
    this.messageSource = messageSource;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    timestamp = input.readLong();
    messageSource = input.readUTF();
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeLong(timestamp);
    output.writeUTF(messageSource);
  }

  @Override
  public int compareTo(MessageKey key) {
    if (timestamp > key.timestamp)
      return 1;
    else if (timestamp < key.timestamp)
      return -1;
    return messageSource.compareTo(key.messageSource);
  }

  @Override
  public int hashCode() {
    return new Long(timestamp).hashCode();
  }

}

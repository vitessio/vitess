package com.github.youtube.vitess.jdbc.bson;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.net.Socket;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Factory for creating a {@link java.net.Socket} connected to the {@code vtocc} server specified by
 * {@code vtoccServerSpec}.
 */
public class SocketFactory {

  public Socket connect(String vtoccServerSpec) throws IOException {
    Preconditions.checkNotNull(vtoccServerSpec, "vtoccServerSpec");
    Matcher matcher = Pattern.compile("(.*):(\\d+)").matcher(vtoccServerSpec);
    Preconditions.checkArgument(matcher.matches());

    String address = matcher.group(1);
    try {
      int port = Integer.parseInt(matcher.group(2));
      return new Socket(address, port, null, 0);
    } catch (NumberFormatException e) {
      throw new RuntimeException("vtoccServerSpec port matches int but can not be parsed", e);
    }
  }
}

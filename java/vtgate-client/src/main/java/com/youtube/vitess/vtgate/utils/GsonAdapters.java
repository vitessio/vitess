package com.youtube.vitess.vtgate.utils;

import java.io.IOException;

import com.google.common.primitives.UnsignedLong;
import com.google.gson.JsonSyntaxException;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

/**
 * Custom GSON adapters for {@link UnsignedLong} and {@link Class} types
 */
public class GsonAdapters {
  public static final TypeAdapter<UnsignedLong> UNSIGNED_LONG = new TypeAdapter<UnsignedLong>() {
    @Override
    public UnsignedLong read(JsonReader in) throws IOException {
      if (in.peek() == JsonToken.NULL) {
        in.nextNull();
        return null;
      }
      try {
        return UnsignedLong.valueOf(in.nextString());
      } catch (NumberFormatException e) {
        throw new JsonSyntaxException(e);
      }
    }

    @Override
    public void write(JsonWriter out, UnsignedLong value) throws IOException {
      out.value(value.toString());
    }
  };

  public static final TypeAdapter<Class> CLASS = new TypeAdapter<Class>() {
    @Override
    public Class read(JsonReader in) throws IOException {
      if (in.peek() == JsonToken.NULL) {
        in.nextNull();
        return null;
      }
      try {
        return Class.forName(in.nextString());
      } catch (NumberFormatException | ClassNotFoundException e) {
        throw new JsonSyntaxException(e);
      }
    }

    @Override
    public void write(JsonWriter out, Class value) throws IOException {
      out.value(value.getName());
    }
  };
}

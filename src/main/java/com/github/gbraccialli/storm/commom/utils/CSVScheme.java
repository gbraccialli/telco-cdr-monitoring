package com.github.gbraccialli.storm.commom.utils;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class CSVScheme implements Scheme {
  private String[] fields;
  private String delim;

  public CSVScheme(String[] _fields, String _delim){
    fields = _fields;
    if (_delim != null) delim = _delim;
    else delim = ",";
  }

  public List<Object> deserialize(byte[] bytes) { return new Values(deserializeString(bytes)); }

  public String[] deserializeString(byte[] string) {
    try { return new String(string, "UTF-8").split(delim); }
    catch (UnsupportedEncodingException e) { throw new RuntimeException(e); }
  }

  public Fields getOutputFields() { return new Fields(fields); }
}

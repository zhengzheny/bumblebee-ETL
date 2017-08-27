/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gsta.bigdata.etl.flume.sources;

import com.google.common.collect.Lists;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.ResettableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * 第一层是.tar.gz文件，下面是目录，目录下面又是.gz
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class TarGZGZMROLineDeserializer implements EventDeserializer {

  private static final Logger logger = LoggerFactory.getLogger(TarGZGZMROLineDeserializer.class);

  private final ResettableTarFileInputStream in;
  private TarArchiveEntry tarArchiveEntry = null;

  private final Charset outputCharset;
  private final int maxLineLength;
  private volatile boolean isOpen;

  public static final String OUT_CHARSET_KEY = "outputCharset";
  public static final String CHARSET_DFLT = "UTF-8";

  public static final String MAXLINE_KEY = "maxLineLength";
  public static final int MAXLINE_DFLT = 2048;

  //tar.gz文件是否结束
  private boolean isEndFile = false;
  //文件读取缓冲区
  private BufferedReader bufReader;
  private  String inputCharset;

  //只处理MRO文件
  public static final String MRO_FILE_NAME = "_MRO_";

  TarGZGZMROLineDeserializer(Context context, ResettableInputStream in, String inputCharset) {
    this.in = (ResettableTarFileInputStream)in;

    this.inputCharset = inputCharset;

    this.outputCharset = Charset.forName(
            context.getString(OUT_CHARSET_KEY, CHARSET_DFLT));
    this.maxLineLength = context.getInteger(MAXLINE_KEY, MAXLINE_DFLT);
    this.isOpen = true;
  }

  /**
   * Reads a line from a file and returns an event
   * @return Event containing parsed line
   * @throws IOException
   */
  @Override
  public Event readEvent() throws IOException {
    ensureOpen();
    String line = readLine();

    if (line == null) {
      return null;
    } else {
      //line=new String(line.getBytes("gbk"),"utf-8");
      return EventBuilder.withBody(line, outputCharset);
    }
  }

  /**
   * Batch line read
   * @param numEvents Maximum number of events to return.
   * @return List of events containing read lines
   * @throws IOException
   */
  @Override
  public List<Event> readEvents(int numEvents) throws IOException {
    ensureOpen();
    List<Event> events = Lists.newLinkedList();
    for (int i = 0; i < numEvents; i++) {
      Event event = readEvent();
      if (event != null) {
        events.add(event);
      } else if (this.isEndFile){
        break;
      }
    }
    return events;
  }

  @Override
  public void mark() throws IOException {
    ensureOpen();
    in.mark();
  }

  @Override
  public void reset() throws IOException {
    ensureOpen();
    in.reset();
  }

  @Override
  public void close() throws IOException {
    if (isOpen) {
      reset();
      in.close();
      isOpen = false;
    }

    //buffered reader 不用关闭，里面会自动关闭input stream
    //if(bufReader != null) bufReader.close();
  }

  private void ensureOpen() {
    if (!isOpen) {
      throw new IllegalStateException("Serializer has been closed");
    }
  }

  private String readLine() throws IOException {
    if(this.isEndFile)  return null;

    if(bufReader != null){
      String line = this.bufReader.readLine();
      if(line != null) {
        return line;
      }else{
        //换压缩包的下一个文件
        this.tarArchiveEntry = this.in.getTarIs().getNextTarEntry();
        if(this.tarArchiveEntry != null && !this.tarArchiveEntry.isDirectory()
                && this.tarArchiveEntry.getName().contains(MRO_FILE_NAME)) {
          bufReader = new BufferedReader(new InputStreamReader(
                  new GZIPInputStream(this.in.getTarIs()), inputCharset));
          logger.info("file name is " + this.tarArchiveEntry.getName());
        }else if(this.tarArchiveEntry == null){
          logger.info("finish read tar.gz file");
          this.isEndFile = true;
        }
      }
    }else {
      getNextFile();
    }

    return null;
  }

  private void getNextFile() throws IOException{
    //如果是目录忽略掉
    if(this.tarArchiveEntry == null || this.tarArchiveEntry.isDirectory()){
      this.tarArchiveEntry = this.in.getTarIs().getNextTarEntry();
      if(this.tarArchiveEntry != null) {
        //考虑多层目录情况
        while (this.tarArchiveEntry.isDirectory()){
          logger.info("file name is " + this.tarArchiveEntry.getName());
          this.tarArchiveEntry = this.in.getTarIs().getNextTarEntry();
        }

        String fileName = this.tarArchiveEntry.getName();

        if(!this.tarArchiveEntry.isDirectory() && fileName.contains(MRO_FILE_NAME)) {
          bufReader = new BufferedReader(new InputStreamReader(
                  new GZIPInputStream(this.in.getTarIs()), inputCharset));
          logger.info("file name is " + fileName);
        }
      }else{
        logger.info("finish read tar.gz file");
        this.isEndFile = true;
      }
    }
  }

  public static class Builder implements EventDeserializer.Builder {

    @Override
    public EventDeserializer build(Context context, ResettableInputStream in) {
      String inputCharset = context.getString("inputCharset");
      return new TarGZGZMROLineDeserializer(context, in,inputCharset);
    }

  }

  public static void main(String[] args) {
    File file = new File("D:\\github\\bumblebee-ETL\\doc\\LTE_MRGZ_HUAWEI_132.122.151.91_201708030930_201708030945_001.tar.gz");
    try {
      TarArchiveInputStream tais =
              new TarArchiveInputStream(
                      new GzipCompressorInputStream(new FileInputStream(file)), "utf-8");

      TarArchiveEntry tarArchiveEntry ;
      while ((tarArchiveEntry=tais.getNextTarEntry()) != null) {
        if(tarArchiveEntry.isDirectory()) continue;

        System.out.println(tarArchiveEntry.getName());
        /*BufferedReader bufReader = new BufferedReader(
                new InputStreamReader(tais, "utf-8"));*/
        BufferedReader bufReader = new BufferedReader(
                new InputStreamReader(new GZIPInputStream(tais), "utf-8"));
        String line ;
        while((line = bufReader.readLine()) != null){
        //  System.out.println(line);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}

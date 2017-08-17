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
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.ResettableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * A deserializer that parses text lines from a file.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ZipLineDeserializer implements EventDeserializer {

    private static final Logger logger = LoggerFactory.getLogger(ZipLineDeserializer.class);

    private final ResettableZipFileInputStream in;
    private ZipEntry zipEntry = null;

    private final Charset outputCharset;
    private final int maxLineLength;
    private volatile boolean isOpen;

    public static final String OUT_CHARSET_KEY = "outputCharset";
    public static final String CHARSET_DFLT = "UTF-8";

    public static final String MAXLINE_KEY = "maxLineLength";
    public static final int MAXLINE_DFLT = 2048;

    //zip文件是否结束
    private boolean isEndFile = false;
    //文件读取缓冲区
    private BufferedReader bufReader;

    ZipLineDeserializer(Context context, ResettableInputStream in) {
        this.in = (ResettableZipFileInputStream)in;

        //初始化第一个entry
        try {
            this.zipEntry = this.in.getZipIs().getNextEntry();
            logger.info("file name is " + this.zipEntry.getName());
        } catch (IOException e) {
            e.printStackTrace();
        }

        bufReader = new BufferedReader(new InputStreamReader(this.in.getZipIs()));

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
    }

    private void ensureOpen() {
        if (!isOpen) {
            throw new IllegalStateException("Serializer has been closed");
        }
    }

    // TODO: consider not returning a final character that is a high surrogate
    // when truncating
    private String readLine() throws IOException {
        //如果是目录忽略掉
        if(this.zipEntry == null || this.zipEntry.isDirectory()){
            this.zipEntry = this.in.getZipIs().getNextEntry();
            if(this.zipEntry != null) {
                logger.info("file name is " + this.zipEntry.getName());
            }else{
                logger.info("finish read .zip file");
                this.isEndFile = true;
                return null;
            }
        }

        String line = this.bufReader.readLine();
        if(line != null) {
            return line;
        }else{
            //换压缩包的下一个文件
            this.zipEntry = this.in.getZipIs().getNextEntry();
            if(this.zipEntry != null) {
                logger.info("file name is " + this.zipEntry.getName());
            }
            return null;
        }
    }

    public static class Builder implements EventDeserializer.Builder {
        @Override
        public EventDeserializer build(Context context, ResettableInputStream in) {
            return new ZipLineDeserializer(context, in);
        }

    }

}


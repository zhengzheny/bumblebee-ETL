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
import org.apache.commons.io.IOUtils;
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
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * 处理第一层是.zip文件，第二层还是zip，zip里面是未压缩文件
 * zip.zip.file和tar.gz.file处理不一样，需要对单独拿出来处理
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ZipZipMROLineDeserializer implements EventDeserializer {

    private static final Logger logger = LoggerFactory.getLogger(ZipZipMROLineDeserializer.class);

    private final ResettableZipFileInputStream in;
    private ZipEntry zipEntry = null;

    private final Charset outputCharset;
    private final int maxLineLength;
    private volatile boolean isOpen;

    public static final String OUT_CHARSET_KEY = "outputCharset";
    public static final String CHARSET_DFLT = "UTF-8";

    public static final String MAXLINE_KEY = "maxLineLength";
    public static final int MAXLINE_DFLT = 2048;
    //只需要MRO文件
    public static final String MRO_FILE_NAME = "_MRO_";

    //zip文件是否结束
    private boolean isEndFile = false;
    //文件读取缓冲区
    private BufferedReader bufReader;
    //第二层
    private ZipInputStream zin2 = null;

    ZipZipMROLineDeserializer(Context context, ResettableInputStream in) {
        this.in = (ResettableZipFileInputStream)in;

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

        //buffered reader 不用关闭，里面会自动关闭input stream
        //if(bufReader != null) bufReader.close();
        if(zin2 != null) {
            logger.info("close zin2");
            zin2.close();
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
        if(isEndFile)  return null;

        if(bufReader != null){
            String line = this.bufReader.readLine();
            if(line != null) {
                return line;
            }else{
                //换压缩包的下一个文件
                getNextFile();
            }
        }else{
            //初始化
            getNextFile();
        }

        return null;
    }

    private  void getNextFile()  throws IOException{
        //如果第二级的压缩包没处理完，继续下一个
        if(zin2 != null){
            ZipEntry ze2 = zin2.getNextEntry();
            if(ze2 != null ){
                bufReader = new BufferedReader(new InputStreamReader(zin2));
                return;
            }else{
                //第二级没有了，转入第一级下一个
                this.zipEntry = this.in.getZipIs().getNextEntry();
                if(this.zipEntry != null) {
                    while (this.zipEntry.isDirectory()){
                        this.zipEntry = this.in.getZipIs().getNextEntry();
                    }

                    String fileName = this.zipEntry.getName();
                    if(!this.zipEntry.isDirectory() && fileName.contains(MRO_FILE_NAME)) {
                        zin2 = createZipInputStream(this.in.getZipIs());
                        ZipEntry ze21 = zin2.getNextEntry();
                        if(ze21 != null && ze21.getName().contains(MRO_FILE_NAME)){
                            logger.info("file name is " + fileName);
                            bufReader = new BufferedReader(new InputStreamReader(zin2));
                        }
                    }
                }else{
                    logger.info("finish read .zip file");
                    this.isEndFile = true;
                }
            }
        }

        //处理第一级压缩
        if(this.zipEntry == null || this.zipEntry.isDirectory() ||
                !this.zipEntry.getName().contains(MRO_FILE_NAME)){
            this.zipEntry = this.in.getZipIs().getNextEntry();
            if(this.zipEntry != null) {
                while (this.zipEntry.isDirectory()){
                    this.zipEntry = this.in.getZipIs().getNextEntry();
                }

                String fileName = this.zipEntry.getName();
                if(!this.zipEntry.isDirectory() && fileName.contains(MRO_FILE_NAME)) {
                    zin2 = createZipInputStream(this.in.getZipIs());
                    ZipEntry ze2 = zin2.getNextEntry();
                    if(ze2 != null && ze2.getName().contains(MRO_FILE_NAME)){
                        logger.info("file name is " + fileName);
                        bufReader = new BufferedReader(new InputStreamReader(zin2));
                    }
                }
            }else{
                logger.info("finish read .zip file");
                this.isEndFile = true;
            }
        }
    }

    public static class Builder implements EventDeserializer.Builder {
        @Override
        public EventDeserializer build(Context context, ResettableInputStream in) {
            return new ZipZipMROLineDeserializer(context, in);
        }

    }

    public static ZipInputStream createZipInputStream(ZipInputStream zin) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copy(zin, out);
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        return new ZipInputStream(in);
    }

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ZipZipMROLineDeserializer.class);

        String file = "D:\\github\\noceDataStructure\\mrosource\\FDD-LTE_MR_ZTE_OMC1_20170803164500.zip";
        try {
            ZipInputStream zipis = new ZipInputStream(new FileInputStream(file), Charset.forName("utf-8"));

            ZipEntry zipEntry;
            while ((zipEntry = zipis.getNextEntry()) != null) {
                if (zipEntry.isDirectory() ||
                        !zipEntry.getName().contains(ZipZipMROLineDeserializer.MRO_FILE_NAME))
                    continue;

                logger.info(zipEntry.getName());

                ZipInputStream zin2 = createZipInputStream(zipis);

                ZipEntry ze2;
                while ((ze2 = zin2.getNextEntry()) != null) {
                    BufferedReader bufReader = new BufferedReader(
                            new InputStreamReader(zin2));
                    String line;
                    while ((line = bufReader.readLine()) != null) {
                        logger.info(line);
                    }
                    //buffered reader 不用关闭，里面会自动关闭input stream
                    //bufReader.close();
                }
                zin2.close();
            }
            zipis.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}



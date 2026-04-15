/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.*;

public class TestParquetReaderListEncodings {

  static class RecordWithList {
    List<Integer> listField;

    RecordWithList(int seed) {
      this.listField = IntStream.range(0, seed).boxed().collect(Collectors.toList());
    }

    RecordWithList(List<Integer> list) {
      this.listField = list;
    }

    @Override
    public String toString() {
      return "RecordWithList{" + listField + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      RecordWithList that = (RecordWithList) o;
      return Objects.equals(listField, that.listField);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(listField);
    }
  }

  private static final List<RecordWithList> DATA =
      IntStream.range(0, 10).mapToObj(RecordWithList::new).collect(Collectors.toList());

  static class TestListEncoding {
    private String label;
    private MessageType schema;
    private BiFunction<RecordWithList, RecordConsumer, Void> writeFn;
    private Function<Group, RecordWithList> materializeFn;

    TestListEncoding(
        String label,
        MessageType schema,
        BiFunction<RecordWithList, RecordConsumer, Void> writeFn,
        Function<Group, RecordWithList> materializeFn) {
      this.label = label;
      this.schema = schema;
      this.writeFn = writeFn;
      this.materializeFn = materializeFn;
    }

    WriteSupport<RecordWithList> getWriteSupport() {
      return new WriteSupport<>() {
        RecordConsumer rc = null;

        @Override
        public WriteContext init(Configuration configuration) {
          return new WriteContext(schema, new HashMap<>());
        }

        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
          this.rc = recordConsumer;
        }

        @Override
        public void write(RecordWithList record) {
          writeFn.apply(record, rc);
        }
      };
    }

    RecordWithList materialize(Group group) {
      return materializeFn.apply(group);
    }
  }

  private static final java.nio.file.Path TEMP_DIR;

  static {
    try {
      TEMP_DIR = Files.createTempDirectory("parquet-reader-list-encoding");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // Todo fill in all the valid encodings from
  // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
  private static final List<TestListEncoding> VALID_ENCODINGS = List.of(
      new TestListEncoding(
          "3-level",
          MessageTypeParser.parseMessageType("message org.apache.parquet.hadoop.RecordWithListPrimitive {\n"
              + "  required group listField (LIST) {\n"
              + "     repeated group list {\n"
              + "       required int32 element (INTEGER(32,true));\n"
              + "     }\n"
              + "  }\n"
              + "}"),
          (record, rc) -> {
            rc.startMessage();

            rc.startField("listField", 0);
            rc.startGroup();

            if (!record.listField.isEmpty()) {
              rc.startField("list", 0);
              record.listField.forEach((elem) -> {
                rc.startGroup();

                rc.startField("element", 0);
                rc.addInteger(elem);
                rc.endField("element", 0);

                rc.endGroup();
              });

              rc.endField("list", 0);
            }
            rc.endGroup();
            rc.endField("listField", 0);

            rc.endMessage();
            return null;
          },
          (group) -> {
            final Group listField = group.getGroup("listField", 0);
            final List<Integer> elements = new ArrayList<>();
            int i = 0;
            while (true) {
              try {
                elements.add(listField.getGroup("list", i).getInteger("element", 0));
                i++;
              } catch (Exception e) {
                break; // List is empty
              }
            }
            return new RecordWithList(elements);
          }),
      new TestListEncoding(
          "1-level",
          MessageTypeParser.parseMessageType("message org.apache.parquet.hadoop.RecordWithListPrimitive {\n"
              + "  repeated int32 listField (INTEGER(32,true));\n"
              + "}"),
          (record, rc) -> {
            rc.startMessage();

            if (!record.listField.isEmpty()) {
              rc.startField("listField", 0);
              record.listField.forEach(rc::addInteger);
              rc.endField("listField", 0);
            }

            rc.endMessage();
            return null;
          },
          group -> {
            final List<Integer> elements = new ArrayList<>();
            int i = 0;
            while (true) {
              try {
                elements.add(group.getInteger("listField", i));
                i++;
              } catch (Exception e) {
                break; // List is emptied
              }
            }
            return new RecordWithList(elements);
          }));

  static class LocalWriteBuilder extends ParquetWriter.Builder<RecordWithList, LocalWriteBuilder> {
    private final TestListEncoding encoding;

    LocalWriteBuilder(TestListEncoding encoding) {
      this.encoding = encoding;
    }

    @Override
    protected LocalWriteBuilder self() {
      return this;
    }

    @Override
    protected WriteSupport<RecordWithList> getWriteSupport(Configuration conf) {
      return encoding.getWriteSupport();
    }
  }

  static class LocalReaderBuilder extends ParquetReader.Builder<RecordWithList> {
    private final TestListEncoding encoding;

    LocalReaderBuilder(TestListEncoding encoding) {
      this.encoding = encoding;
    }

    @Override
    protected ReadSupport<RecordWithList> getReadSupport() {
      return super.getReadSupport();
    }
  }

  @BeforeClass
  public static void createFiles() throws IOException {
    for (TestListEncoding encoding : VALID_ENCODINGS) {
      final LocalOutputFile outputFile = new LocalOutputFile(TEMP_DIR.resolve(encoding.label + ".parquet"));
      final ParquetWriter<RecordWithList> writer =
          new LocalWriteBuilder(encoding).withFile(outputFile).build();
      DATA.forEach(record -> {
        try {
          writer.write(record);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
      writer.close();
    }
  }

  @AfterClass
  public static void deleteFiles() throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR.toFile());
  }

  // Test that 3-level list encoding can be read with all valid encoding types
  @Test
  public void testThreeLevelListEncoding() throws Exception {
    final TestListEncoding writeEncoding = VALID_ENCODINGS.stream()
        .filter(e -> e.label.equals("3-level"))
        .findFirst()
        .get();

    final Path inputPath = new Path(TEMP_DIR.toString(), writeEncoding.label + ".parquet");

    for (TestListEncoding readEncoding : VALID_ENCODINGS) {
      final Configuration conf = new Configuration();
      GroupWriteSupport.setSchema(readEncoding.schema, conf);

      final ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), inputPath)
          .withConf(conf)
          .build();

      final List<RecordWithList> results = IntStream.range(0, 10)
          .mapToObj(i -> {
            try {
              return readEncoding.materialize(reader.read());
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format(
                      "Reading list data with write encoding %s and read encoding %s failed",
                      writeEncoding.label, readEncoding.label),
                  e);
            }
          })
          .collect(Collectors.toList());
      Assert.assertEquals(
          String.format(
              "Failed to read list data when write schema used encoding %s and read schema used encoding %s",
              writeEncoding.label, readEncoding.label),
          DATA,
          results);
    }
  }

  // Test that 1-level list encoding can be read with all valid encoding types
  @Test
  public void testOneLevelListEncoding() throws Exception {
    final TestListEncoding writeEncoding = VALID_ENCODINGS.stream()
        .filter(e -> e.label.equals("1-level"))
        .findFirst()
        .get();

    final Path inputPath = new Path(TEMP_DIR.toString(), writeEncoding.label + ".parquet");

    for (TestListEncoding readEncoding : VALID_ENCODINGS) {
      final Configuration conf = new Configuration();
      GroupWriteSupport.setSchema(readEncoding.schema, conf);

      final ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), inputPath)
          .withConf(conf)
          .build();

      final List<RecordWithList> results = IntStream.range(0, 10)
          .mapToObj(i -> {
            try {
              return readEncoding.materialize(reader.read());
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format(
                      "Reading list data with write encoding %s and read encoding %s failed",
                      writeEncoding.label, readEncoding.label),
                  e);
            }
          })
          .collect(Collectors.toList());
      Assert.assertEquals(
          String.format(
              "Failed to read list data when write schema used encoding %s and read schema used encoding %s",
              writeEncoding.label, readEncoding.label),
          DATA,
          results);
    }
  }
}

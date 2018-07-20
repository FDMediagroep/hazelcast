/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.nio.serialization;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CustomSmileBasedSerializationTest {

    // tests based on blog-post:
    // Custom Serialization using Jackson Smile
    // https://blog.hazelcast.com/comparing-serialization-methods/  (
    // June 13, 2013

    // in order to prevent issues it is important to
    // implement ByteArraySerializer instead of StreamSerializer

    @Test
    public void testSerializer() {
        SerializationConfig config = new SerializationConfig();
        config.setUseNativeByteOrder(false);
        SerializerConfig sc = new SerializerConfig()
                .setImplementation(new FooSmileSerializer())
                .setTypeClass(Foo.class);
        config.addSerializerConfig(sc);
        SerializationService ss = new DefaultSerializationServiceBuilder().setConfig(config).build();
        Foo foo = new Foo("f");
        Data d = ss.toData(foo);
        Foo newFoo = ss.toObject(d);

        assertEquals(newFoo, foo);
    }

    @Test
    public void testSequenceOfTwoSerializations() {
        SerializationConfig config = new SerializationConfig();
        config.setUseNativeByteOrder(false);
        SerializerConfig sc = new SerializerConfig()
                .setImplementation(new FooSmileSerializer())
                .setTypeClass(Foo.class);
        config.addSerializerConfig(sc);
        SerializationService ss = new DefaultSerializationServiceBuilder().setConfig(config).build();
        Foo foo = new Foo("f");
        Data d = ss.toData(foo);
        Foo newFoo = ss.toObject(d);

        Data d2 = ss.toData(foo);
        Foo newFoo2 = ss.toObject(d2);

        assertEquals(newFoo, foo);
        assertEquals(newFoo2, foo);
    }

    @Test
    public void testSerializerVersioning() {
        SerializationConfig config = new SerializationConfig();
        config.setUseNativeByteOrder(false);
        SerializerConfig sc = new SerializerConfig()
                .setImplementation(new FooSmileSerializer())
                .setTypeClass(Foo.class);
        config.addSerializerConfig(sc);
        SerializationService ss = new DefaultSerializationServiceBuilder().setConfig(config).build();

        Foo foo = new Foo("f");
        foo.setName("name");
        Data d = ss.toData(foo);
        Foo newFoo = ss.toObject(d);

        assertEquals(newFoo, foo);

        SerializationConfig config2 = new SerializationConfig();
        config2.setUseNativeByteOrder(false);
        SerializerConfig sc2 = new SerializerConfig()
                .setImplementation(new Foo2SmileSerializer())
                .setTypeClass(Foo2.class);
        config2.addSerializerConfig(sc2);
        SerializationService ss2 = new DefaultSerializationServiceBuilder().setConfig(config2).build();

        Foo2 newFoo2 = ss2.toObject(d);
        assertEquals("f", newFoo2.foo);
        assertNull(newFoo2.getTitle());
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Foo {

        private String foo;
        private String name;

        public Foo() {
        }

        public Foo(String foo) {
            this.foo = foo;
        }

        public String getFoo() {
            return foo;
        }

        public void setFoo(String foo) {
            this.foo = foo;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Foo foo1 = (Foo) o;

            return !(foo != null ? !foo.equals(foo1.foo) : foo1.foo != null);

        }

        @Override
        public int hashCode() {
            return foo != null ? foo.hashCode() : 0;
        }
    }

    public static class FooSmileSerializer implements ByteArraySerializer<Foo> {

        private SmileFactory f = new SmileFactory();
        private ObjectMapper mapper = new ObjectMapper(f);

        @Override
        public int getTypeId() {
            return 10;
        }

        @Override
        public byte[] write(Foo foo) throws IOException {
            return mapper.writeValueAsBytes(foo);
        }

        @Override
        public Foo read(byte[] bytes) throws IOException {
            return mapper.readValue(bytes, Foo.class);
        }

        @Override
        public void destroy() {
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Foo2 {

        private String foo;
        private String title;

        public Foo2() {
        }

        public Foo2(String foo) {
            this.foo = foo;
        }

        public String getFoo() {
            return foo;
        }

        public void setFoo(String foo) {
            this.foo = foo;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Foo foo1 = (Foo) o;

            return !(foo != null ? !foo.equals(foo1.foo) : foo1.foo != null);

        }

        @Override
        public int hashCode() {
            return foo != null ? foo.hashCode() : 0;
        }
    }

    public static class Foo2SmileSerializer implements ByteArraySerializer<Foo2> {

        private SmileFactory f = new SmileFactory();
        private ObjectMapper mapper = new ObjectMapper(f);

        @Override
        public int getTypeId() {
            return 10;
        }

        @Override
        public byte[] write(Foo2 foo) throws IOException {
            return mapper.writeValueAsBytes(foo);
        }

        @Override
        public Foo2 read(byte[] bytes) throws IOException {
            return mapper.readValue(bytes, Foo2.class);
        }

        @Override
        public void destroy() {
        }
    }
}

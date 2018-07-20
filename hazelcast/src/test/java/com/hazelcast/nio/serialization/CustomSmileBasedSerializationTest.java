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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CustomSmileBasedSerializationTest {

    // tests based on blog-post:
    // Custom Serialization using Jackson Smile
    // https://blog.hazelcast.com/comparing-serialization-methods/  (
    // June 13, 2013

    // Issue 1:
    // a null object is returned instead of the original
    @Test
    public void testSerializerIssue1() {
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

    // Issue 2: reuse of buffers in BufferPoolImpl fails
    //	at com.hazelcast.internal.serialization.impl.SerializationUtil.handleSerializeException(SerializationUtil.java:75)
    //	at com.hazelcast.internal.serialization.impl.AbstractSerializationService.toBytes(AbstractSerializationService.java:157)
    //	at com.hazelcast.internal.serialization.impl.AbstractSerializationService.toBytes(AbstractSerializationService.java:133)
    //	at com.hazelcast.internal.serialization.impl.AbstractSerializationService.toData(AbstractSerializationService.java:118)
    //	at com.hazelcast.internal.serialization.impl.AbstractSerializationService.toData(AbstractSerializationService.java:106)
    // ..
    // Caused by: java.lang.NullPointerException
    //	at com.hazelcast.internal.serialization.impl.ByteArrayObjectDataOutput.position(ByteArrayObjectDataOutput.java:399)
    //	at com.hazelcast.internal.serialization.impl.AbstractSerializationService.toBytes(AbstractSerializationService.java:144)
    @Test
    public void testSerializerIssue2() {
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

        // second time: reuse of buffer
        Data d2 = ss.toData(foo);
        Foo newFoo2 = ss.toObject(d2);

        assertEquals(newFoo, foo);
        assertEquals(newFoo2, foo);
    }

    public static class Foo {

        private String foo;

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

    public static class FooSmileSerializer implements StreamSerializer<Foo> {

        private SmileFactory f = new SmileFactory();
        private ObjectMapper mapper = new ObjectMapper(f);

        AtomicInteger serializationCount = new AtomicInteger();

        @Override
        public int getTypeId() {
            return 10;
        }

        @Override
        public void write(ObjectDataOutput out, Foo object) throws IOException {
            serializationCount.incrementAndGet();
            mapper.writeValue((OutputStream) out, object);
            ((OutputStream) out).flush();
        }

        @Override
        public Foo read(ObjectDataInput in) throws IOException {
            return mapper.readValue((InputStream) in, Foo.class);
        }

        @Override
        public void destroy() {
        }
    }
}

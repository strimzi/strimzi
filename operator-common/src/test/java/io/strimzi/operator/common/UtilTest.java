/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.strimzi.operator.common.Util.matchesSelector;
import static io.strimzi.operator.common.Util.parseMap;
import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class UtilTest {
    @Test
    public void testParseMap() {
        String stringMap = "key1=value1\n" +
                "key2=value2";

        Map<String, String> m = parseMap(stringMap);
        assertThat(m, aMapWithSize(2));
        assertThat(m, hasEntry("key1", "value1"));
        assertThat(m, hasEntry("key2", "value2"));
    }

    @Test
    public void testParseMapNull() {
        Map<String, String> m = parseMap(null);
        assertThat(m, aMapWithSize(0));
    }

    @Test
    public void testParseMapEmptyString() {
        String stringMap = "";

        Map<String, String> m = parseMap(null);
        assertThat(m, aMapWithSize(0));
    }

    @Test
    public void testParseMapEmptyValue() {
        String stringMap = "key1=value1\n" +
                "key2=";

        Map<String, String> m = parseMap(stringMap);
        assertThat(m, aMapWithSize(2));
        assertThat(m, hasEntry("key1", "value1"));
        assertThat(m, hasEntry("key2", ""));
    }

    @Test
    public void testParseMapInvalid() {
        assertThrows(RuntimeException.class, () -> {
            String stringMap = "key1=value1\n" +
                    "key2";

            Map<String, String> m = parseMap(stringMap);
            assertThat(m, aMapWithSize(2));
            assertThat(m, hasEntry("key1", "value1"));
            assertThat(m, hasEntry("key2", ""));
        });
    }

    @Test
    public void testParseMapValueWithEquals() {
        String stringMap = "key1=value1\n" +
                "key2=value2=value3";

        Map<String, String> m = parseMap(stringMap);
        assertThat(m, aMapWithSize(2));
        assertThat(m, hasEntry("key1", "value1"));
        assertThat(m, hasEntry("key2", "value2=value3"));
    }

    @Test
    public void testMaskedPasswords()   {
        String noPassword = "SOME_VARIABLE";
        String passwordAtTheEnd = "SOME_PASSWORD";
        String passwordInTheMiddle = "SOME_PASSWORD_TO_THE_BIG_SECRET";

        assertThat(Util.maskPassword(noPassword, "123456"), is("123456"));
        assertThat(Util.maskPassword(passwordAtTheEnd, "123456"), is("********"));
        assertThat(Util.maskPassword(passwordInTheMiddle, "123456"), is("********"));
    }

    @Test
    public void testMergeLabelsOrAnnotations()  {
        Map<String, String> base = new HashMap<>();
        base.put("label1", "value1");
        base.put(Labels.STRIMZI_DOMAIN + "label2", "value2");

        Map<String, String> overrides1 = new HashMap<>();
        overrides1.put("override1", "value1");
        overrides1.put(Labels.KUBERNETES_DOMAIN + "override2", "value2");

        Map<String, String> overrides2 = new HashMap<>();
        overrides2.put("override3", "value3");

        Map<String, String> forbiddenOverrides = new HashMap<>();
        forbiddenOverrides.put(Labels.STRIMZI_DOMAIN + "override4", "value4");

        Map<String, String> expected = new HashMap<>();
        expected.put("label1", "value1");
        expected.put(Labels.STRIMZI_DOMAIN + "label2", "value2");
        expected.put("override1", "value1");
        expected.put("override3", "value3");

        assertThat(Util.mergeLabelsOrAnnotations(base, overrides1, overrides2), is(expected));
        assertThat(Util.mergeLabelsOrAnnotations(base, overrides1, null, overrides2), is(expected));
        assertThat(Util.mergeLabelsOrAnnotations(base, null), is(base));
        assertThat(Util.mergeLabelsOrAnnotations(base), is(base));
        assertThat(Util.mergeLabelsOrAnnotations(null, overrides2), is(overrides2));
        assertThrows(InvalidResourceException.class, () -> Util.mergeLabelsOrAnnotations(base, forbiddenOverrides));
    }

    @Test
    public void testVarExpansion() {
        String input = "log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender\n" +
                "log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout\n" +
                "log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %p %m (%c) [%t]%n\n" +
                "mirrormaker.root.logger=INFO\n" +
                "log4j.rootLogger=${mirrormaker.root.logger}, CONSOLE";

        String expectedOutput = "log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender\n" +
                "log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout\n" +
                "log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %p %m (%c) [%t]%n\n" +
                "mirrormaker.root.logger=INFO\n" +
                "log4j.rootLogger=INFO, CONSOLE\n";
        String result = Util.expandVars(input);
        assertThat(result, is(expectedOutput));
    }

    @Test
    public void testMatchesSelector()   {
        Pod testResource = new PodBuilder()
                .withNewMetadata()
                    .withName("test-pod")
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .build();

        // Resources without any labels
        Optional<LabelSelector> selector = Optional.empty();
        assertThat(matchesSelector(selector, testResource), is(true));

        selector = Optional.of(new LabelSelectorBuilder().withMatchLabels(emptyMap()).build());
        assertThat(matchesSelector(selector, testResource), is(true));

        selector = Optional.of(new LabelSelectorBuilder().withMatchLabels(Map.of("label2", "value2")).build());
        assertThat(matchesSelector(selector, testResource), is(false));

        // Resources with Labels
        testResource.getMetadata().setLabels(Map.of("label1", "value1", "label2", "value2"));

        selector = Optional.empty();
        assertThat(matchesSelector(selector, testResource), is(true));

        selector = Optional.of(new LabelSelectorBuilder().withMatchLabels(emptyMap()).build());
        assertThat(matchesSelector(selector, testResource), is(true));

        selector = Optional.of(new LabelSelectorBuilder().withMatchLabels(Map.of("label2", "value2")).build());
        assertThat(matchesSelector(selector, testResource), is(true));

        selector = Optional.of(new LabelSelectorBuilder().withMatchLabels(Map.of("label2", "value2", "label1", "value1")).build());
        assertThat(matchesSelector(selector, testResource), is(true));

        selector = Optional.of(new LabelSelectorBuilder().withMatchLabels(Map.of("label2", "value1")).build());
        assertThat(matchesSelector(selector, testResource), is(false));

        selector = Optional.of(new LabelSelectorBuilder().withMatchLabels(Map.of("label3", "value3")).build());
        assertThat(matchesSelector(selector, testResource), is(false));

        selector = Optional.of(new LabelSelectorBuilder().withMatchLabels(Map.of("label2", "value2", "label1", "value1", "label3", "value3")).build());
        assertThat(matchesSelector(selector, testResource), is(false));
    }
}

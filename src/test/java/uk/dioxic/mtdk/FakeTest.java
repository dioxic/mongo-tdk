package uk.dioxic.mtdk;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class FakeTest {

    @Test
    void assertTrue_True() {
        Assertions.assertThat(true).isTrue();
    }
}

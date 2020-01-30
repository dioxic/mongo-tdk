package uk.dioxic.mtdk.operator;

import uk.dioxic.mgenerate.common.Resolvable;
import uk.dioxic.mgenerate.common.annotation.Operator;
import uk.dioxic.mgenerate.common.annotation.OperatorProperty;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Operator
public class Imsi implements Resolvable<Long> {

    @OperatorProperty
    List<Integer> prefixes;

    private AtomicInteger seq = new AtomicInteger();

    @Override
    public Long resolve() {
        int i = seq.getAndIncrement();
        Integer imsiPrefix = prefixes.get(i % prefixes.size());
        return (imsiPrefix * 10000000000L) + i;
    }
}
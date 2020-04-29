package uk.dioxic.mtdk.operator;

import uk.dioxic.mgenerate.common.Initializable;
import uk.dioxic.mgenerate.common.Resolvable;
import uk.dioxic.mgenerate.common.annotation.Operator;
import uk.dioxic.mgenerate.common.annotation.OperatorProperty;
import uk.dioxic.mgenerate.core.operator.AbstractOperator;
import uk.dioxic.mgenerate.core.operator.general.Choose;
import uk.dioxic.mgenerate.core.operator.general.ChooseBuilder;
import uk.dioxic.mgenerate.core.util.FakerUtil;
import uk.dioxic.mgenerate.core.util.ResolverUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Operator
public class CustomerId extends AbstractOperator<Object> implements Initializable {

    @OperatorProperty
    Integer customers = 1000;

    @OperatorProperty
    Integer ratio = 1000;

    private List<?> from;

    @Override
    public Object resolveInternal() {
        return from.get(FakerUtil.random().nextInt(from.size()));
    }

    @Override
    public void initialize() {
        from = IntStream.range(1, customers)
                .boxed()
                .collect(Collectors.toList());
        Integer[] weights = new Integer[from.size()];
        Arrays.fill(weights, 1);
        weights[0] = ratio;
        from = ResolverUtil.getWeightedArray(from, Arrays.asList(weights));
    }

}
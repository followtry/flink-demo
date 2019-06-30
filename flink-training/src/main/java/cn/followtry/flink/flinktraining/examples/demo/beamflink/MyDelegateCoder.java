package cn.followtry.flink.flinktraining.examples.demo.beamflink;

import cn.followtry.app.UserInfo;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Objects;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/30
 */
public class MyDelegateCoder<T,IntermediateT> extends CustomCoder<T> {

    @Override
    public void encode(T value, OutputStream outStream) throws CoderException, IOException {
        encode(value, outStream, Coder.Context.NESTED);
    }

    @Override
    public void encode(T value, OutputStream outStream, Coder.Context context)
            throws CoderException, IOException {
        coder.encode(applyAndWrapExceptions(toFn, value), outStream, context);
    }

    @Override
    public T decode(InputStream inStream) throws CoderException, IOException {
        return decode(inStream, Coder.Context.NESTED);
    }

    @Override
    public T decode(InputStream inStream, Coder.Context context) throws CoderException, IOException {
        return applyAndWrapExceptions(fromFn, coder.decode(inStream, context));
    }

    /**
     * Returns the coder used to encode/decode the intermediate values produced/consumed by the coding
     * functions of this {@code MyDelegateCoder}.
     */
    public Coder<IntermediateT> getCoder() {
        return coder;
    }

    /**
     * {@inheritDoc}
     *
     * @throws Coder.NonDeterministicException when the underlying coder's {@code verifyDeterministic()}
     *     throws a {@link Coder.NonDeterministicException}. For this to be safe, the intermediate
     *     {@code CodingFunction2<T, IntermediateT>} must also be deterministic.
     */
    @Override
    public void verifyDeterministic() throws Coder.NonDeterministicException {
        coder.verifyDeterministic();
    }

    /**
     * {@inheritDoc}
     *
     * @return a structural for a value of type {@code T} obtained by first converting to {@code
     *     IntermediateT} and then obtaining a structural value according to the underlying coder.
     */
    @Override
    public Object structuralValue(T value) {
        try {
            IntermediateT intermediate = toFn.apply(value);
            return coder.structuralValue(intermediate);
        } catch (Exception exn) {
            throw new IllegalArgumentException(
                    "Unable to encode element '" + value + "' with coder '" + this + "'.", exn);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || this.getClass() != o.getClass()) {
            return false;
        }
        MyDelegateCoder<?, ?> that = (MyDelegateCoder<?, ?>) o;
        return Objects.equal(this.coder, that.coder)
                && Objects.equal(this.toFn, that.toFn)
                && Objects.equal(this.fromFn, that.fromFn);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.coder, this.toFn, this.fromFn);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass())
                .add("coder", coder)
                .add("toFn", toFn)
                .add("fromFn", fromFn)
                .toString();
    }

    @Override
    public TypeDescriptor<T> getEncodedTypeDescriptor() {
        if (typeDescriptor == null) {
            return super.getEncodedTypeDescriptor();
        }
        return typeDescriptor;
    }

    private String delegateEncodingId(Class<?> delegateClass, String encodingId) {
        return String.format("%s:%s", delegateClass.getName(), encodingId);
    }

    /////////////////////////////////////////////////////////////////////////////

    private <InputT, OutputT> OutputT applyAndWrapExceptions(
            CodingFunction2<InputT, OutputT> fn, InputT input) throws CoderException, IOException {
        try {
            return fn.apply(input);
        } catch (IOException exc) {
            throw exc;
        } catch (Exception exc) {
            throw new CoderException(exc);
        }
    }

    private final Coder<IntermediateT> coder;
    private final CodingFunction2<T, IntermediateT> toFn;
    private final CodingFunction2<IntermediateT, T> fromFn;

    // null unless the user explicitly provides a TypeDescriptor.
    // If null, then the machinery from the superclass (StructuredCoder) will be used
    // to try to deduce a good type descriptor.
    @Nullable private final TypeDescriptor<T> typeDescriptor;

    protected MyDelegateCoder(
            Coder<IntermediateT> coder,
            CodingFunction2<T, IntermediateT> toFn,
            CodingFunction2<IntermediateT, T> fromFn,
            @Nullable TypeDescriptor<T> typeDescriptor) {
        this.coder = coder;
        this.fromFn = fromFn;
        this.toFn = toFn;
        this.typeDescriptor = typeDescriptor;
    }

    public static MyDelegateCoder of(Coder<UserInfo> inputCoder, CntBeanFn.FromUserInfoCodingFunction fromUserInfoCodingFunction, CntBeanFn.ToNameCountCodingFunction toNameCountCodingFunction) {
        MyDelegateCoder coder = new MyDelegateCoder(inputCoder, fromUserInfoCodingFunction, toNameCountCodingFunction, null);
        System.out.println("MyDelegateCoder="+coder);
        return coder;
    }
}

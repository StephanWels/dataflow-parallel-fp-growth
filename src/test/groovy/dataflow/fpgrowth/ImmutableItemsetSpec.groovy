package dataflow.fpgrowth
import com.openpojo.reflection.PojoClass
import com.openpojo.reflection.impl.PojoClassFactory
import com.openpojo.validation.Validator
import com.openpojo.validation.ValidatorBuilder
import com.openpojo.validation.rule.impl.*
import nl.jqno.equalsverifier.EqualsVerifier
import nl.jqno.equalsverifier.Warning
import org.mutabilitydetector.unittesting.AllowedReason
import org.mutabilitydetector.unittesting.MutabilityAssert
import spock.lang.Specification

import static org.mutabilitydetector.unittesting.MutabilityMatchers.areImmutable

class ImmutableItemsetSpec extends Specification {

    def getClazz() {
        ImmutableItemset.class
    }

    def "fulfill immutable contract"() {
        expect:
        MutabilityAssert.assertInstancesOf(getClazz(), areImmutable(),
                AllowedReason.assumingFields("items").areNotModifiedAndDoNotEscape());
    }

    def "fulfill POJO contract"() {
        given:
        final PojoClass pojos = PojoClassFactory.getPojoClass(getClazz())
        final Validator validator = ValidatorBuilder.create()
                .with(new GetterMustExistRule())
                .with(new NoFieldShadowingRule())
                .with(new NoNestedClassRule())
                .with(new NoPublicFieldsExceptStaticFinalRule())
                .with(new NoStaticExceptFinalRule())
                .with(new SerializableMustHaveSerialVersionUIDRule())
                .build();
        expect:
        validator.validate(pojos);
    }

    def "fulfill equals and hashCode contract"() {
        expect:
        EqualsVerifier.forClass(getClazz())
                .suppress(Warning.NULL_FIELDS)
                .verify()
    }

}
package com.stewel.dataflow.assocrules

import com.openpojo.reflection.PojoClass
import com.openpojo.reflection.impl.PojoClassFactory
import com.openpojo.validation.Validator
import com.openpojo.validation.ValidatorBuilder
import com.openpojo.validation.rule.impl.GetterMustExistRule
import com.openpojo.validation.rule.impl.NoFieldShadowingRule
import com.openpojo.validation.rule.impl.NoNestedClassRule
import com.openpojo.validation.rule.impl.NoPublicFieldsExceptStaticFinalRule
import com.openpojo.validation.rule.impl.NoStaticExceptFinalRule
import com.openpojo.validation.rule.impl.SerializableMustHaveSerialVersionUIDRule
import nl.jqno.equalsverifier.EqualsVerifier
import nl.jqno.equalsverifier.Warning
import org.mutabilitydetector.unittesting.AllowedReason
import org.mutabilitydetector.unittesting.MutabilityAssert
import spock.lang.Specification

import static org.mutabilitydetector.unittesting.MutabilityMatchers.areImmutable

class ImmutableAssociationRulesSpec extends Specification {

    def getClazz() {
        ImmutableAssociationRules.class
    }

    def "fulfill immutable contract"() {
        expect:
        MutabilityAssert.assertInstancesOf(getClazz(), areImmutable(),
                AllowedReason.provided(AssociationRule.class).isAlsoImmutable());
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
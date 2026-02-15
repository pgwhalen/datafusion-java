package org.apache.arrow.datafusion;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaField;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;

/** Enforces the FFI naming conventions from .claude/rules/ffi.md. */
@AnalyzeClasses(
    packages = "org.apache.arrow.datafusion",
    importOptions = ImportOption.DoNotIncludeTests.class)
public class FfiNamingTest {

  /** FunctionDescriptor fields must end with _DESC (ffi.md naming conventions). */
  @ArchTest
  static final ArchRule functionDescriptorFieldsShouldEndWithDesc =
      classes()
          .should(haveFieldsOfTypeEndingWith("java.lang.foreign.FunctionDescriptor", "_DESC"))
          .because(
              "static FunctionDescriptor fields must end with _DESC"
                  + " (ffi.md naming conventions)");

  /** UpcallStub fields must end with Stub (ffi.md naming conventions). */
  @ArchTest
  static final ArchRule upcallStubFieldsShouldEndWithStub =
      classes()
          .should(haveFieldsOfTypeEndingWith("org.apache.arrow.datafusion.UpcallStub", "Stub"))
          .because(
              "instance UpcallStub fields must end with Stub" + " (ffi.md naming conventions)");

  private static ArchCondition<JavaClass> haveFieldsOfTypeEndingWith(
      String typeName, String requiredSuffix) {
    return new ArchCondition<>(
        "have fields of type " + typeName + " ending with '" + requiredSuffix + "'") {
      @Override
      public void check(JavaClass javaClass, ConditionEvents events) {
        for (JavaField field : javaClass.getFields()) {
          if (field.getRawType().getName().equals(typeName)
              && !field.getName().endsWith(requiredSuffix)) {
            events.add(
                SimpleConditionEvent.violated(
                    field,
                    String.format(
                        "Field '%s' in %s is of type %s but does not end with '%s'",
                        field.getName(), javaClass.getName(), typeName, requiredSuffix)));
          }
        }
      }
    };
  }
}

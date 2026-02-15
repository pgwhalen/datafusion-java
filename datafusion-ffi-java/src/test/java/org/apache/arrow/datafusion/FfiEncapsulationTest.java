package org.apache.arrow.datafusion;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaConstructor;
import com.tngtech.archunit.core.domain.JavaMember;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Enforces the FFI encapsulation rules from .claude/rules/ffi.md. */
@AnalyzeClasses(
    packages = "org.apache.arrow.datafusion",
    importOptions = ImportOption.DoNotIncludeTests.class)
public class FfiEncapsulationTest {

  private static final Set<String> FFI_CLASS_SIMPLE_NAMES =
      Stream.of(
              NativeUtil.class,
              NativeLoader.class,
              Errors.class,
              UpcallStub.class,
              TraitHandle.class,
              NativeString.class,
              PointerOut.class,
              LongOut.class)
          .map(Class::getSimpleName)
          .collect(Collectors.toUnmodifiableSet());

  private static final DescribedPredicate<JavaClass> IS_FFI_CLASS =
      new DescribedPredicate<>("an FFI implementation class") {
        @Override
        public boolean test(JavaClass javaClass) {
          String name = javaClass.getSimpleName();
          return name.endsWith("Ffi")
              || name.endsWith("Handle")
              || FFI_CLASS_SIMPLE_NAMES.contains(name);
        }
      };

  /** Rule 1: FFI classes should be package-private. */
  @ArchTest
  static final ArchRule ffiClassesShouldBePackagePrivate =
      classes()
          .that(IS_FFI_CLASS)
          .should()
          .notBePublic()
          .because(
              "FFI implementation classes must be package-private"
                  + " to hide FFI details from consumers (ffi.md rule 1)");

  /** Rule 2: Public classes should not depend on java.lang.foreign types. */
  @ArchTest
  static final ArchRule publicClassesShouldNotDependOnForeignApi =
      noClasses()
          .that()
          .arePublic()
          .should()
          .dependOnClassesThat()
          .resideInAPackage("java.lang.foreign..")
          .because("public API classes must not expose java.lang.foreign types (ffi.md rule 2)");

  /** Rule 3: Public methods and constructors should not use MemorySegment. */
  @ArchTest
  static final ArchRule publicMethodsShouldNotUseMemorySegment =
      classes()
          .that()
          .arePublic()
          .should(notHavePublicMembersUsingMemorySegment())
          .because("MemorySegment must not appear in any public API signature (ffi.md rule 3)");

  /** Rule 4: Only FFI classes may depend on NativeUtil. */
  @ArchTest
  static final ArchRule nativeUtilConfinedToFfiClasses =
      noClasses()
          .that(DescribedPredicate.not(IS_FFI_CLASS))
          .should()
          .dependOnClassesThat()
          .haveSimpleName("NativeUtil")
          .because(
              "NativeUtil should only be used" + " by FFI implementation classes (ffi.md rule 4)");

  /** Rule 6: Constructors taking *Ffi parameters must be package-private. */
  @ArchTest
  static final ArchRule publicConstructorsShouldNotAcceptFfiTypes =
      classes()
          .that()
          .arePublic()
          .should(notHavePublicConstructorsAcceptingFfiTypes())
          .because("constructors taking *Ffi types must be package-private (ffi.md rule 6)");

  /** Handle class rule: Handle classes must implement TraitHandle. */
  @ArchTest
  static final ArchRule handleClassesShouldImplementTraitHandle =
      classes()
          .that()
          .haveSimpleNameEndingWith("Handle")
          .and()
          .areNotInterfaces()
          .should()
          .implement(TraitHandle.class)
          .because("every Handle class must implement TraitHandle (ffi.md Handle class rules)");

  /** Handle class rule: Handle classes must be final. */
  @ArchTest
  static final ArchRule handleClassesShouldBeFinal =
      classes()
          .that()
          .haveSimpleNameEndingWith("Handle")
          .and()
          .areNotInterfaces()
          .should()
          .haveModifier(JavaModifier.FINAL)
          .because("Handle classes must be final (ffi.md Handle class rules)");

  private static ArchCondition<JavaClass> notHavePublicMembersUsingMemorySegment() {
    return new ArchCondition<>("not have public members using MemorySegment") {
      @Override
      public void check(JavaClass javaClass, ConditionEvents events) {
        for (JavaMethod method : javaClass.getMethods()) {
          if (!method.getModifiers().contains(JavaModifier.PUBLIC)) {
            continue;
          }
          checkTypes(method, method.getRawParameterTypes(), "parameter", events);
          checkType(method, method.getRawReturnType(), "return type", events);
        }
        for (JavaConstructor constructor : javaClass.getConstructors()) {
          if (!constructor.getModifiers().contains(JavaModifier.PUBLIC)) {
            continue;
          }
          checkTypes(constructor, constructor.getRawParameterTypes(), "parameter", events);
        }
      }

      private void checkTypes(
          JavaMember member, List<JavaClass> types, String kind, ConditionEvents events) {
        for (JavaClass type : types) {
          checkType(member, type, kind, events);
        }
      }

      private void checkType(
          JavaMember member, JavaClass type, String kind, ConditionEvents events) {
        if (type.getName().equals("java.lang.foreign.MemorySegment")) {
          events.add(
              SimpleConditionEvent.violated(
                  member,
                  String.format(
                      "%s in %s has MemorySegment as %s",
                      member.getDescription(), member.getOwner().getName(), kind)));
        }
      }
    };
  }

  private static ArchCondition<JavaClass> notHavePublicConstructorsAcceptingFfiTypes() {
    return new ArchCondition<>("not have public constructors accepting *Ffi types") {
      @Override
      public void check(JavaClass javaClass, ConditionEvents events) {
        for (JavaConstructor constructor : javaClass.getConstructors()) {
          if (!constructor.getModifiers().contains(JavaModifier.PUBLIC)) {
            continue;
          }
          for (JavaClass paramType : constructor.getRawParameterTypes()) {
            if (paramType.getSimpleName().endsWith("Ffi")) {
              events.add(
                  SimpleConditionEvent.violated(
                      constructor,
                      String.format(
                          "%s in %s has public constructor accepting FFI type %s",
                          constructor.getDescription(),
                          javaClass.getName(),
                          paramType.getSimpleName())));
            }
          }
        }
      }
    };
  }
}

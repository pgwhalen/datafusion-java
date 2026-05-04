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

/**
 * Enforces encapsulation rules for the Diplomat-based FFI architecture.
 *
 * <p>Diplomat-generated classes live in the {@code generated} subpackage. Bridge classes (*Bridge),
 * adapter classes (Df*Adapter), remaining FFI classes (*Ffi), and utility classes (NativeUtil,
 * Errors) are internal implementation details. Public API classes must not expose java.lang.foreign
 * types.
 */
@AnalyzeClasses(
    packages = "org.apache.arrow.datafusion",
    importOptions = ImportOption.DoNotIncludeTests.class)
public class DiplomatEncapsulationTest {

  private static final String FFI_PACKAGE = "org.apache.arrow.datafusion";
  private static final String GENERATED_PACKAGE = "org.apache.arrow.datafusion.generated";

  private static final Set<String> UTILITY_CLASS_NAMES =
      Set.of("NativeUtil", "NativeLoader", "Errors", "BridgeUtil", "ArrowFfiUtil");

  private static final DescribedPredicate<JavaClass> IS_INTERNAL_FFI_CLASS =
      new DescribedPredicate<>("an internal FFI/bridge/adapter/generated class") {
        @Override
        public boolean test(JavaClass javaClass) {
          if (javaClass.getPackageName().equals(GENERATED_PACKAGE)) {
            return true;
          }
          if (!javaClass.getPackageName().startsWith(FFI_PACKAGE)) {
            return false;
          }
          String name = javaClass.getSimpleName();
          return name.endsWith("Ffi")
              || name.endsWith("Bridge")
              || name.endsWith("Converter")
              || isDiplomatAdapter(name)
              || UTILITY_CLASS_NAMES.contains(name);
        }

        private boolean isDiplomatAdapter(String name) {
          return name.startsWith("Df") && name.endsWith("Adapter");
        }
      };

  /**
   * Matches inner classes of internal FFI classes (e.g., DfCatalogTrait.Statics). These are
   * implicitly public in Java (interface members) but are implementation details of their enclosing
   * class.
   */
  private static final DescribedPredicate<JavaClass> IS_INNER_OF_FFI_CLASS =
      new DescribedPredicate<>("an inner class of an internal FFI class") {
        @Override
        public boolean test(JavaClass javaClass) {
          return javaClass.getEnclosingClass().map(IS_INTERNAL_FFI_CLASS::test).orElse(false);
        }
      };

  /**
   * Rule 2: Public classes should not depend on java.lang.foreign types.
   *
   * <p>The FFM API (MemorySegment, Arena, ValueLayout, etc.) must only appear in package-private
   * implementation classes.
   */
  @ArchTest
  static final ArchRule publicClassesShouldNotDependOnForeignApi =
      noClasses()
          .that()
          .arePublic()
          .and(DescribedPredicate.not(IS_INTERNAL_FFI_CLASS))
          .and(DescribedPredicate.not(IS_INNER_OF_FFI_CLASS))
          .should()
          .dependOnClassesThat()
          .resideInAPackage("java.lang.foreign..")
          .because("public API classes must not expose java.lang.foreign types");

  /**
   * Rule 3: Public methods and constructors should not use MemorySegment in their signatures.
   *
   * <p>This prevents FFM types from leaking into the public API.
   */
  @ArchTest
  static final ArchRule publicMethodsShouldNotUseMemorySegment =
      classes()
          .that()
          .arePublic()
          .and(DescribedPredicate.not(IS_INTERNAL_FFI_CLASS))
          .should(notHavePublicMembersUsingMemorySegment())
          .because("MemorySegment must not appear in any public API signature");

  /**
   * Rule 4: Only internal FFI classes may depend on NativeLoader.
   *
   * <p>NativeLoader is the native library loading mechanism and should be confined to FFI/bridge
   * classes.
   */
  @ArchTest
  static final ArchRule nativeLoaderConfinedToInternalClasses =
      noClasses()
          .that(DescribedPredicate.not(IS_INTERNAL_FFI_CLASS))
          .and(DescribedPredicate.not(IS_INNER_OF_FFI_CLASS))
          .should()
          .dependOnClassesThat()
          .haveSimpleName("NativeLoader")
          .because("NativeLoader should only be used by internal FFI/bridge classes");

  /**
   * Names of public types that are intentionally allowed to return Diplomat-generated {@code Df*}
   * handles to internal callers. This is a documented carve-out (see
   * {@code .claude/ffi-violations.md}); the rule still catches any new cases.
   */
  private static final Set<String> KNOWN_GENERATED_TYPE_RETURNERS =
      Set.of("RustCatalogProvider", "RustTableProvider");

  private static final DescribedPredicate<JavaClass> IS_KNOWN_GENERATED_RETURNER =
      new DescribedPredicate<>("a class whose Df*-returning getter is documented as intentional") {
        @Override
        public boolean test(JavaClass javaClass) {
          return KNOWN_GENERATED_TYPE_RETURNERS.contains(javaClass.getSimpleName());
        }
      };

  /**
   * Rule 5: Public API methods (on classes that are not internal FFI/bridge/adapter/generated) must
   * not return Diplomat-generated {@code Df*} types. The bridge layer is responsible for
   * encapsulating those.
   *
   * <p>{@code RustCatalogProvider} and {@code RustTableProvider} are listed as documented carve-outs
   * — their {@code handle()} accessors are needed by {@code SessionContextBridge} in a different
   * package, and refactoring this would require significant package restructuring.
   */
  @ArchTest
  static final ArchRule publicApiMethodsShouldNotReturnGeneratedTypes =
      classes()
          .that()
          .arePublic()
          .and(DescribedPredicate.not(IS_INTERNAL_FFI_CLASS))
          .and(DescribedPredicate.not(IS_INNER_OF_FFI_CLASS))
          .and(DescribedPredicate.not(IS_KNOWN_GENERATED_RETURNER))
          .should(notHavePublicMembersReturningGeneratedType())
          .because("public API classes must not expose Diplomat-generated Df* types");

  private static ArchCondition<JavaClass> notHavePublicMembersReturningGeneratedType() {
    return new ArchCondition<>("not have public methods returning Diplomat-generated Df* types") {
      @Override
      public void check(JavaClass javaClass, ConditionEvents events) {
        for (JavaMethod method : javaClass.getMethods()) {
          if (!method.getModifiers().contains(JavaModifier.PUBLIC)) {
            continue;
          }
          JavaClass returnType = method.getRawReturnType();
          if (returnType.getPackageName().equals(GENERATED_PACKAGE)) {
            events.add(
                SimpleConditionEvent.violated(
                    method,
                    String.format(
                        "%s returns Diplomat-generated %s",
                        method.getDescription(), returnType.getSimpleName())));
          }
        }
      }
    };
  }

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
}

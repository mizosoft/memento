/*
 * Copyright (c) 2019, 2020 Moataz Abdelnasser
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.github.mizonas.memento;

import static com.github.mizonas.memento.internal.Validate.requireArgument;
import static java.util.Objects.requireNonNull;

import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An object that represents the {@link Type} of the generic argument {@code T}. This class utilizes
 * the supertype-token idiom, which is used to capture complex types (i.e. generic types) that are
 * otherwise impossible to represent using ordinary {@code Class} objects.
 *
 * @param <T> the type this object represents
 */
public abstract class TypeRef<T> {
  private final Type type;
  private @MonotonicNonNull Class<?> rawType;

  /**
   * Creates a new {@code TypeRef<T>} capturing the {@code Type} of {@code T}. It is usually the
   * case that this constructor is invoked as an anonymous class expression (e.g. {@code new
   * TypeRef<List<String>>() {}}).
   *
   * @throws IllegalStateException if the raw version of this class is used
   */
  protected TypeRef() {
    Type superClass = getClass().getGenericSuperclass();
    //    requireState(superClass instanceof ParameterizedType, "not used in parameterized form");
    this.type = ((ParameterizedType) superClass).getActualTypeArguments()[0];
  }

  private TypeRef(Type type) {
    this.type = type;
    rawType = findRawType(type);
  }

  /** Returns the underlying Java {@link Type}. */
  public final Type type() {
    return type;
  }

  /**
   * Returns the {@code Class} object that represents the resolved raw type of {@code T}. The
   * returned class is {@code Class<? super T>} because {@code T} can possibly be a generic type,
   * and it is not semantically correct for a {@code Class} to be parameterized with such.
   *
   * @see #exactRawType()
   */
  @SuppressWarnings("unchecked")
  public final Class<? super T> rawType() {
    Class<?> clz = rawType;
    if (clz == null) {
      try {
        clz = findRawType(type);
      } catch (IllegalArgumentException e) {
        // rawType is lazily initialized only if not user-provided and
        // on that case findRawType shouldn't fail
        throw new AssertionError("couldn't get raw type of: " + type, e);
      }
      rawType = clz;
    }
    return (Class<? super T>) clz;
  }

  /**
   * Returns the underlying type as a {@code Class<T>} for when it is known that {@link T} is
   * already raw. Similar to {@code (Class<T>) typeRef.type()}.
   *
   * @throws UnsupportedOperationException if the underlying type is not a raw type
   */
  @SuppressWarnings("unchecked")
  public final Class<T> exactRawType() {
    if (!(type instanceof Class<?>)) {
      throw new UnsupportedOperationException("<" + type + "> is not a raw type");
    }
    return (Class<T>) type;
  }

  /**
   * Returns {@code true} if the given object is a {@code TypeRef} and both instances represent the
   * same type.
   *
   * @param obj the object to test for equality
   */
  @Override
  public boolean equals(@Nullable Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof TypeRef)) {
      return false;
    }
    return type.equals(((TypeRef<?>) obj).type);
  }

  @Override
  public int hashCode() {
    return 31 * type.hashCode();
  }

  /** Returns a string representation for the type. */
  @Override
  public String toString() {
    return type.getTypeName();
  }

  private static Class<?> findRawType(Type type) {
    if (type instanceof Class) {
      return (Class<?>) type;
    } else if (type instanceof ParameterizedType) {
      var rawType = ((ParameterizedType) type).getRawType();
      requireArgument(
          rawType instanceof Class,
          "ParameterizedType::getRawType of %s returned a non-raw type: %s",
          type,
          rawType);
      return (Class<?>) rawType;
    } else if (type instanceof GenericArrayType) {
      // Here, the raw type is the type of the array created with the generic-component's raw type
      var rawComponentType = findRawType(((GenericArrayType) type).getGenericComponentType());
      return Array.newInstance(rawComponentType, 0).getClass();
    } else if (type instanceof TypeVariable) {
      return rawUpperBound(((TypeVariable<?>) type).getBounds());
    } else if (type instanceof WildcardType) {
      return rawUpperBound(((WildcardType) type).getUpperBounds());
    }
    throw new IllegalArgumentException(
        "unsupported specialization of java.lang.reflect.Type: " + type);
  }

  private static Class<?> rawUpperBound(Type[] upperBounds) {
    // Same behaviour as Method::getGenericReturnType vs Method::getReturnType
    return upperBounds.length > 0 ? findRawType(upperBounds[0]) : Object.class;
  }

  /**
   * Creates a new {@code TypeRef} from the given type.
   *
   * @param type the type
   * @throws IllegalArgumentException if the given type is not a standard specialization of a java
   *     {@code Type}
   */
  public static TypeRef<?> from(Type type) {
    return new ExplicitTypeRef<>(type);
  }

  /**
   * Creates a new {@code TypeRef} from the given class.
   *
   * @param rawType the class
   * @param <U> the raw type that the given class represents
   */
  public static <U> TypeRef<U> from(Class<U> rawType) {
    return new ExplicitTypeRef<>(rawType);
  }

  public static <U> TypeRef<U> of(U value) {
    return new ExplicitTypeRef<>(value.getClass());
  }

  private static final class ExplicitTypeRef<T> extends TypeRef<T> {

    ExplicitTypeRef(Type type) {
      super(requireNonNull(type));
    }
  }
}

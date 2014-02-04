using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Xunit.Sdk;

// ReSharper disable once CheckNamespace
namespace Xunit.Should
{
	public static class ShouldExtensions
	{
		public static void ShouldContain(this string self, string str)
		{
			Assert.Contains(str, self);
		}

		public static void ShouldContain(this string self, string str, StringComparison comparison)
		{
			Assert.Contains(str, self, comparison);
		}

		private static string FormatSequence<T>(IEnumerable<T> seq, int markerIndex, string separator = ", ", string markerStart = "-->", string markerEnd = "<--", int maxLength = 100)
		{
			var sb = new StringBuilder("{");
			if(seq == null) return "<null>";
			var enumerator = seq.GetEnumerator();
			var index = 0;
			while(index < maxLength && enumerator.MoveNext())
			{
				if(index > 0)
					sb.Append(separator);
				if(index == markerIndex)
				{
					sb.Append(markerStart);
					sb.Append(enumerator.Current);
					sb.Append(markerEnd);
				}
				else
					sb.Append(enumerator.Current);
				index++;
			}
			if(index == markerIndex)
			{
				sb.Append(markerStart);
				sb.Append(' ');
				sb.Append(markerEnd);
			}
			sb.Append('}');
			return sb.ToString();
		}

		public static void ShouldOnlyContainInOrder<T>(this IEnumerable<T> actual, params T[] expected)
		{
			Func<string, int, string> errorFormat = (msg, index) =>
				string.Format("{3}First difference is at index={2}. {4}Expected: {0}{4}Actual: {1}", FormatSequence(expected, index), FormatSequence(actual, index), index, msg==null ? "": msg+Environment.NewLine, Environment.NewLine);
			
			var i = 0;
			var lastIndex = expected.Length - 1;
			foreach(var item in actual)
			{
				if(i > lastIndex)
				{
					throw new Exception(errorFormat(string.Format("Expected {0} items but actually have more items.",expected.Length),i));
				}
				try
				{
					Assert.Equal(expected[i], item);
				}
				catch(EqualException e)
				{
					throw new Exception(errorFormat(null, i));
				}
				i++;
			}
			if(expected.Length != i)
				throw new Exception(errorFormat(string.Format("Expected {0} items but only have {1} items.", expected.Length, i), i));
		}

		public static void ShouldContain<T>(this IEnumerable<T> series, T item)
		{
			Assert.Contains(item, series);
		}

		public static void ShouldContain<T>(this IEnumerable<T> series, T item, IEqualityComparer<T> comparer)
		{
			Assert.Contains(item, series, comparer);
		}

		public static void ShouldContainKeys<TKey, TValue>(this IReadOnlyDictionary<TKey, TValue> dictionary, params TKey[] expectedKeys)
		{
			foreach(var expectedKey in expectedKeys)
			{
				Assert.True(dictionary.ContainsKey(expectedKey), string.Format("Expected dictionary to contain \"{0}", expectedKey));
			}
		}
		public static void ShouldOnlyContainKeys<TKey, TValue>(this IReadOnlyDictionary<TKey, TValue> dictionary, params TKey[] expectedKeys)
		{
			ShouldContainKeys(dictionary, expectedKeys);
			var actualCount = dictionary.Count;
			Assert.True(actualCount == expectedKeys.Length, string.Format("Expected dictionary to contain only a set of keys.\nExpected: {0} itesm\nActual:   {1} items", expectedKeys.Length, actualCount));
		}

		public static void ShouldNotContain(this string self, string str)
		{
			Assert.DoesNotContain(str, self);
		}

		public static void ShouldNotContain(this string self, string str, StringComparison comparison)
		{
			Assert.DoesNotContain(str, self, comparison);
		}

		public static void ShouldNotContain<T>(this IEnumerable<T> series, T item)
		{
			Assert.DoesNotContain(item, series);
		}

		public static void ShouldNotContain<T>(this IEnumerable<T> series, T item, IEqualityComparer<T> comparer)
		{
			Assert.DoesNotContain(item, series, comparer);
		}

		public static void ShouldBeEmpty(this IEnumerable series)
		{
			Assert.Empty(series);
		}

		public static void ShouldNotBeEmpty(this IEnumerable series)
		{
			Assert.NotEmpty(series);
		}

		public static void ShouldBe<T>(this T self, T other)
		{
			Assert.Equal(other, self);
		}

		public static void ShouldBe<T>(this T self, T other, IEqualityComparer<T> comparer)
		{
			Assert.Equal(other, self, comparer);
		}

		public static void ShouldNotBe<T>(this T self, T other)
		{
			Assert.NotEqual(other, self);
		}

		public static void ShouldNotBe<T>(this T self, T other, IEqualityComparer<T> comparer)
		{
			Assert.NotEqual(other, self, comparer);
		}

		public static void ShouldBeNull(this object self)
		{
			Assert.Null(self);
		}

		public static void ShouldNotBeNull(this object self)
		{
			Assert.NotNull(self);
		}

		public static void ShouldBeSameAs(this object self, object other)
		{
			Assert.Same(other, self);
		}

		public static void ShouldNotBeSameAs(this object self, object other)
		{
			Assert.NotSame(other, self);
		}

		public static void ShouldBeTrue(this bool self)
		{
			Assert.True(self);
		}

		public static void ShouldBeTrue(this bool self, string message)
		{
			Assert.True(self, message);
		}

		public static void ShouldBeFalse(this bool self)
		{
			Assert.False(self);
		}

		public static void ShouldBeFalse(this bool self, string message)
		{
			Assert.False(self, message);
		}

		public static void ShouldBeInRange<T>(this T self, T low, T high) where T : IComparable
		{
			Assert.InRange(self, low, high);
		}

		public static void ShouldNotBeInRange<T>(this T self, T low, T high) where T : IComparable
		{
			Assert.NotInRange(self, low, high);
		}

		public static void ShouldBeGreaterThan<T>(this T self, T other)
			where T : IComparable<T>
		{
			Assert.True(self.CompareTo(other) > 0);
		}

		public static void ShouldBeGreaterThan<T>(this T self, T other, IComparer<T> comparer)
		{
			Assert.True(comparer.Compare(self, other) > 0);
		}

		public static void ShouldBeGreaterThanOrEqualTo<T>(this T self, T other)
			where T : IComparable<T>
		{
			Assert.True(self.CompareTo(other) >= 0);
		}

		public static void ShouldBeGreaterThanOrEqualTo<T>(this T self, T other, IComparer<T> comparer)
		{
			Assert.True(comparer.Compare(self, other) >= 0);
		}

		public static void ShouldBeLessThan<T>(this T self, T other)
			where T : IComparable<T>
		{
			Assert.True(self.CompareTo(other) < 0);
		}

		public static void ShouldBeLessThan<T>(this T self, T other, IComparer<T> comparer)
		{
			Assert.True(comparer.Compare(self, other) < 0);
		}

		public static void ShouldBeLessThanOrEqualTo<T>(this T self, T other)
			where T : IComparable<T>
		{
			Assert.True(self.CompareTo(other) <= 0);
		}

		public static void ShouldBeLessThanOrEqualTo<T>(this T self, T other, IComparer<T> comparer)
		{
			Assert.True(comparer.Compare(self, other) <= 0);
		}

		public static void ShouldBeInstanceOf<T>(this object self)
		{
			Assert.IsType<T>(self);
		}

		public static void ShouldBeInstanceOf(this object self, Type type)
		{
			Assert.IsType(type, self);
		}

		public static void ShouldNotBeInstanceOf<T>(this object self)
		{
			Assert.IsNotType<T>(self);
		}

		public static void ShouldNotBeInstanceOf(this object self, Type type)
		{
			Assert.IsNotType(type, self);
		}

		public static void ShouldBeThrownBy<T>(this T self, Assert.ThrowsDelegate method)
			where T : Exception
		{
			Assert.Throws<T>(method);
		}

		public static void ShouldHaveLength<T>(this T[] arr, int length)
		{
			Assert.Equal(length, arr.Length);
		}

		public static void ShouldHaveCount<T>(this IReadOnlyCollection<T> collection, int count)
		{
			Assert.Equal(count, collection.Count);
		}


		public static void ShouldNotHaveValue<T>(this T? actual) where T : struct
		{
			Assert.False(actual.HasValue,"Expected nullable to be null");
		}

		public static void ShouldHaveValue<T>(this T? actual) where T : struct
		{
			Assert.True(actual.HasValue, "Expected nullable to have a value");
		}

		public static void ShouldHaveValue<T>(this T? actual, T expected) where T : struct
		{
			Assert.Equal(actual.Value,expected);
		}
	}
}

//Copied from https://github.com/damieng/DamienGKit/blob/master/CSharp/DamienG.Library/Security/Cryptography/Crc32.cs   SHA: fc6e4dcbe4861af0410c584ff7e9ee720e768eb5
//Modifications have been made to it


// Copyright (c) Damien Guard.  All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Originally published at http://damieng.com/blog/2006/08/08/calculating_crc32_in_c_and_net

using System;
using System.Collections.Generic;
using System.Security.Cryptography;

namespace KafkaClient.Utils
{
	/// <summary>
	/// Implements a 32-bit CRC hash algorithm compatible with Zip etc.
	/// </summary>
	/// <remarks>
	/// Crc32 should only be used for backward compatibility with older file formats
	/// and algorithms. It is not secure enough for new applications.
	/// If you need to call multiple times for the same data either use the HashAlgorithm
	/// interface or remember that the result of one Compute call needs to be ~ (XOR) before
	/// being passed in as the seed for the next Compute call.
	/// Copyright (c) Damien Guard.  All rights reserved.
	/// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
	/// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	/// Originally published at http://damieng.com/blog/2006/08/08/calculating_crc32_in_c_and_net
	/// </remarks>
	public sealed class Crc32 : ICrcHasher
	{
		public const UInt32 DefaultPolynomial = 0xedb88320u;
		public const UInt32 DefaultSeed = 0xffffffffu;

		private static readonly UInt32[] _DefaultTable;
		private static readonly Crc32 _Instance;

		static Crc32()
		{
			_DefaultTable = CreateTable(DefaultPolynomial);
			_Instance = new Crc32();
		}

		private Crc32() {/* Intentionally left blank */}

		public static Crc32 Instance
		{
			get { return _Instance; }
		}

		public static UInt32 Compute(byte[] buffer)
		{
			return Compute(DefaultSeed, buffer);
		}

		public static UInt32 Compute(UInt32 seed, byte[] buffer)
		{
			return Compute(DefaultPolynomial, seed, buffer);
		}

		public static UInt32 Compute(UInt32 polynomial, UInt32 seed, byte[] buffer)
		{
			return Compute(polynomial, seed, buffer, 0, buffer.Length);
		}

		public uint ComputeCrc(byte[] buffer, int offset, int count)
		{
			return Compute(buffer, offset, count);
		}

		public static uint Compute(byte[] buffer, int offset, int count)
		{
			return ~CalculateHash(_DefaultTable, DefaultSeed, buffer, offset, count);
		}

		public static uint Compute(uint polynomial, uint seed, byte[] buffer, int offset, int count)
		{
			return ~CalculateHash(InitializeTable(polynomial), seed, buffer, offset, count);
		}

		private static UInt32[] InitializeTable(UInt32 polynomial)
		{
			if(polynomial == DefaultPolynomial)
				return _DefaultTable;

			var table = CreateTable(polynomial);
			return table;
		}

		private static uint[] CreateTable(uint polynomial)
		{
			var table = new UInt32[256];
			for(var i = 0; i < 256; i++)
			{
				var entry = (UInt32)i;
				for(var j = 0; j < 8; j++)
					if((entry & 1) == 1)
						entry = (entry >> 1) ^ polynomial;
					else
						entry = entry >> 1;
				table[i] = entry;
			}
			return table;
		}

		private static UInt32 CalculateHash(UInt32[] table, UInt32 seed, IList<byte> buffer, int offset, int count)
		{
			var crc = seed;
			var lastIndex = offset + count - 1;
			for(var i = offset; i <= lastIndex; i++)
				crc = (crc >> 8) ^ table[buffer[i] ^ crc & 0xff];
			return crc;
		}
	}
}

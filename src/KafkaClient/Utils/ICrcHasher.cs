﻿namespace Kafka.Client.Utils
{
	public interface ICrcHasher
	{
		uint ComputeCrc(byte[] buffer, int offset, int count);
	}
}
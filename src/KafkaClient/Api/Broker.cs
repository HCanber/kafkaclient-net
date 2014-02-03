using System;
using Kafka.Client.IO;

namespace Kafka.Client.Api
{
	public class Broker : IEquatable<Broker>
	{
		private readonly int _nodeId;
		private readonly HostPort _host;


		public Broker(int nodeId, string host, ushort port) : this(nodeId, new HostPort(host, port))
		{
		}

		public Broker(int nodeId, HostPort host)
		{
			_nodeId = nodeId;
			_host = host;
		}

		public int NodeId
		{
			get { return _nodeId; }
		}

		public HostPort Host
		{
			get { return _host; }
		}

		public static Broker Deserialize(IReadBuffer buffer)
		{
			var nodeId = buffer.ReadInt();
			var host = buffer.ReadShortString();
			var port = (ushort) buffer.ReadIntInRange(0, ushort.MaxValue, "port");
			return new Broker(nodeId,new HostPort(host,port));
		}

		public bool Equals(Broker other)
		{
			if(ReferenceEquals(null, other)) return false;
			if(ReferenceEquals(this, other)) return true;
			return _nodeId == other._nodeId && _host.Equals(other._host);
		}

		public override bool Equals(object obj)
		{
			if(ReferenceEquals(null, obj)) return false;
			if(ReferenceEquals(this, obj)) return true;
			if(obj.GetType() != GetType()) return false;
			return Equals((Broker) obj);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				var hashCode = _nodeId;
				hashCode = (hashCode*397) ^ _host.GetHashCode();
				return hashCode;
			}
		}

		public static bool operator ==(Broker left, Broker right)
		{
			return Equals(left, right);
		}

		public static bool operator !=(Broker left, Broker right)
		{
			return !Equals(left, right);
		}
	}
}
using System;
using System.Collections;
using System.Globalization;
using System.Text;

namespace Kafka.Client
{
	public class HostPort : IEquatable<HostPort>, IComparable<HostPort>, IStructuralComparable, IStructuralEquatable
	{
		private readonly string _host;
		private readonly ushort _port;

		public HostPort(string host, ushort port)
		{
			if(host == null) throw new ArgumentNullException("host");
			if(host.Length==0) throw new ArgumentException("host must be specified","host");
			_host = host;
			_port = port;
		}

		public string Host{get { return _host; }}
		public ushort Port{get { return _port; }}

		public override bool Equals(object obj)
		{
			if(ReferenceEquals(null, obj)) return false;
			if(ReferenceEquals(this, obj)) return true;
			if(obj.GetType() != GetType()) return false;
			return Equals((HostPort) obj);
		}

		public bool Equals(HostPort other)
		{
			if(ReferenceEquals(null, other)) return false;
			if(ReferenceEquals(this, other)) return true;
			return string.Equals(_host, other._host,StringComparison.OrdinalIgnoreCase) && _port == other._port;
		}

		bool IStructuralEquatable.Equals(object other, IEqualityComparer comparer)
		{
			if(ReferenceEquals(null, other)) return false;
			if(ReferenceEquals(this, other)) return true;
			var otherHostPort = other as HostPort;
			return otherHostPort != null
			       && comparer.Equals(_host, otherHostPort._host)
			       && comparer.Equals(_port, otherHostPort._port);
		}


		public int CompareTo(HostPort other)
		{
			if(other == null) return 1;
			var hostComparison = string.Compare(_host,other._host,StringComparison.OrdinalIgnoreCase);
			if(hostComparison != 0) return hostComparison;
			return _port.CompareTo(other._port);
		}

		int IStructuralComparable.CompareTo(object other, IComparer comparer)
		{
			if(other == null) return 1;
			var otherHostPort = other as HostPort;
			if(otherHostPort == null)
			{
				throw new ArgumentException(string.Format("Cannot compare a {0} to a {1}.", GetType(), other.GetType()),"other");
			}
			var hostComparison = comparer.Compare(_host,otherHostPort._host);
			if(hostComparison != 0) return hostComparison;
			return comparer.Compare(_port, otherHostPort._port);
		}

		int IStructuralEquatable.GetHashCode(IEqualityComparer comparer)
		{

			return (comparer.GetHashCode(_host)*397) ^ comparer.GetHashCode(_port);

		}

		public override int GetHashCode()
		{
			unchecked
			{
				return (_host.GetHashCode()*397) ^ _port;
			}
		}

		public static bool operator ==(HostPort left, HostPort right)
		{
			return Equals(left, right);
		}

		public static bool operator !=(HostPort left, HostPort right)
		{
			return !Equals(left, right);
		}

		public override string ToString()
		{
			var port = _port.ToString(CultureInfo.InvariantCulture);
			var sb = new StringBuilder(_host.Length+1+port.Length);
			sb.Append(_host);
			sb.Append(':');
			sb.Append(_port);
			return sb.ToString();
		}
	}
}
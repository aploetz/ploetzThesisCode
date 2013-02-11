package www.aaronstechcenter.com.datatools;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

public class CassandraDAO {
	private Keyspace keyspace;
	private String columnFamilyName;

	// we use a string serializer for the keys
	private final Serializer<String> keySerializer = StringSerializer.get();
	// we use the object serializer for the values
	private final Serializer<String> valueSerializer = StringSerializer.get();

	
	public CassandraDAO(Keyspace keyspace, String columnFamilyName) {
		this.keyspace = keyspace;
		this.columnFamilyName = columnFamilyName;
	}
	
	public void insert(String key, String value) {
		HColumn column = HFactory.createColumn("PRODUCT_INFO", value, keySerializer, valueSerializer);
		Mutator mutator = HFactory.createMutator(keyspace, keySerializer);
		mutator.insert(key, columnFamilyName, column);
	}
}

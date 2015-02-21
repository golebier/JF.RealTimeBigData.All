package com.github.currencies.registrator;

import org.apache.spark.serializer.KryoRegistrator;

import com.esotericsoftware.kryo.Kryo;
import com.github.currencies.holder.CurrenciesHolder;

/**
 * {@code KryoRegistrator} implementation to register correct {@code Kyro}s.
 *  
 * @author gra
 */
public class CurrenciesRegistrator implements KryoRegistrator {
	@Override
	public void registerClasses(Kryo kryo) {
		kryo.register(CurrenciesHolder.class);
	}
}

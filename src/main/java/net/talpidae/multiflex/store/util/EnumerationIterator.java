package net.talpidae.multiflex.store.util;

import java.util.Enumeration;
import java.util.Iterator;


/**
 * Wrap an enumeration into an iterator of same type.
 */
public class EnumerationIterator<E> implements Iterator<E>
{
    private final Enumeration<E> enumeration;

    public EnumerationIterator(Enumeration<E> enumeration)
    {
        this.enumeration = enumeration;
    }


    @Override
    public boolean hasNext()
    {
        return enumeration.hasMoreElements();
    }

    @Override
    public E next()
    {
        return enumeration.nextElement();
    }
}

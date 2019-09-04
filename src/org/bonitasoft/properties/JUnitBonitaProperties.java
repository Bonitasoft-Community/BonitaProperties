package org.bonitasoft.properties;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.properties.BonitaProperties.ItemKeyValue;
import org.junit.Test;

public class JUnitBonitaProperties {

    @Test
    public void TestComposeDecompose() throws Exception {
        List<ItemKeyValue> initialList = new ArrayList<ItemKeyValue>();

        initialList.add(ItemKeyValue.getInstance(23, "MyRessource", "MyDomain", "color", "blue"));

        initialList.add(ItemKeyValue.getInstance(23, "MyRessource", "MyDomain", "length", null));

        String valueVeryLarge = "";
        for (int i = 0; i < 5000; i++) {
            valueVeryLarge += String.valueOf(i) + ",";
        }
        initialList.add(ItemKeyValue.getInstance(23, "MyRessource", "MyDomain", "size", valueVeryLarge));

        final BonitaProperties BonitaProperties = new BonitaProperties("Test");
        final List<ItemKeyValue> listDecomposed = BonitaProperties.decomposeTooLargekey(initialList);
        System.out.println("After decomposition, nb Items in the list=" + listDecomposed.size());
        assert listDecomposed.size() > initialList.size();

        List<ItemKeyValue> listRecomposed = BonitaProperties.recomposeTooLargekey(listDecomposed);

        System.out.println("After recomposition, nb Items in the list=" + listRecomposed.size());
        assert listRecomposed.size() == initialList.size();

        initialList = orderList(initialList);
        listRecomposed = orderList(listRecomposed);
        for (int i = 1; i < initialList.size(); i++) {
            assert initialList.get(i).rsResourceName.equals(listRecomposed.get(i).rsResourceName);
            assert initialList.get(i).rsDomainName.equals(listRecomposed.get(i).rsDomainName);
            assert initialList.get(i).rsKey.equals(listRecomposed.get(i).rsKey);

            if (initialList.get(i).rsValue != null) {
                assert initialList.get(i).rsValue.equals(listRecomposed.get(i).rsValue);
            }

        }
        System.out.println("Test ok");
    }

    private List<ItemKeyValue> orderList(final List<ItemKeyValue> listItem) {
        Collections.sort(listItem, new Comparator<ItemKeyValue>() {

            @Override
            public int compare(final ItemKeyValue s1, final ItemKeyValue s2) {
                return s1.rsKey.compareTo(s2.rsKey);
            }
        });
        return listItem;
    }

    @Test
    public void TestSaveRetrieveKey() throws Exception {

        String valueVeryLarge = "";
        for (int i = 0; i < 5000; i++) {
            valueVeryLarge += String.valueOf(i) + ",";
        }
        String valueNormal = "Hello word";

        BonitaProperties bonitaProperties = new BonitaProperties("Test");

        List<BEvent> listEvents = new ArrayList<BEvent>();
        listEvents.addAll(bonitaProperties.load());
        System.out.println("BonitaProperties.load: loadproperties done, events = " + listEvents.size());

        bonitaProperties.traceInLog();
        bonitaProperties.setProperty("veryLarge", valueVeryLarge);
        bonitaProperties.setProperty("normal", valueNormal);

        listEvents.addAll(bonitaProperties.store());
        System.out.println("BonitaProperties.load: loadproperties done, events = " + listEvents.size());

        listEvents.addAll(bonitaProperties.load());
        System.out.println("BonitaProperties.load: loadproperties done, events = " + listEvents.size());

        bonitaProperties.traceInLog();
        String compareVeryLarge = bonitaProperties.getProperty("veryLarge");
        String compareNormal = bonitaProperties.getProperty(valueNormal);
        assert (compareVeryLarge.equals(valueVeryLarge));
        assert (compareNormal.equals(valueNormal));

    }

    @Test
    public void test() {
        fail("Not yet implemented");
    }

}

package com.flipkart.vitess.jdbc;

import com.flipkart.vitess.util.Constants;
import com.youtube.vitess.proto.Query;
import com.youtube.vitess.proto.Topodata;

import java.lang.reflect.Field;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class ConnectionProperties {

    private static final ArrayList<java.lang.reflect.Field> PROPERTY_LIST = new ArrayList<>();

    static {
        try {
            // Generate property list for use in dynamically filling out values from Properties objects in
            // #initializeProperties below
            java.lang.reflect.Field[] declaredFields = ConnectionProperties.class.getDeclaredFields();
            for (int i = 0; i < declaredFields.length; i++) {
                if (ConnectionProperties.ConnectionProperty.class.isAssignableFrom(declaredFields[i].getType())) {
                    PROPERTY_LIST.add(declaredFields[i]);
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    // Vitess-specific configs
    private EnumConnectionProperty<Constants.QueryExecuteType> executeType = new EnumConnectionProperty<>(
        Constants.Property.EXECUTE_TYPE,
        "Query execution type: simple or stream",
        Constants.DEFAULT_EXECUTE_TYPE);
    private BooleanConnectionProperty twopcEnabled = new BooleanConnectionProperty(
        Constants.Property.TWOPC_ENABLED,
        "Whether to enable two-phased commit, for atomic distributed commits. See http://vitess.io/user-guide/twopc.html",
        false);
    private EnumConnectionProperty<Query.ExecuteOptions.IncludedFields> includedFields = new EnumConnectionProperty<>(
        Constants.Property.INCLUDED_FIELDS,
        "What fields to return from MySQL to the Driver. Limiting the fields returned can improve performance, but ALL is required for maximum JDBC API support",
        Constants.DEFAULT_INCLUDED_FIELDS);
    private EnumConnectionProperty<Topodata.TabletType> tabletType = new EnumConnectionProperty<>(
        Constants.Property.TABLET_TYPE,
        "Tablet Type to which Vitess will connect(master, replica, rdonly)",
        Constants.DEFAULT_TABLET_TYPE);

    // Caching of some hot properties to avoid casting over and over
    private Topodata.TabletType tabletTypeCache;
    private Query.ExecuteOptions.IncludedFields includedFieldsCache;
    private boolean includeAllFieldsCache = true;
    private boolean twopcEnabledCache = false;
    private boolean simpleExecuteTypeCache = true;

    void initializeProperties(Properties props) throws SQLException {
        Properties propsCopy = (Properties) props.clone();
        for (Field propertyField : PROPERTY_LIST) {
            try {
                ConnectionProperty propToSet = (ConnectionProperty) propertyField.get(this);
                propToSet.initializeFrom(propsCopy);
            } catch (IllegalAccessException iae) {
                throw new SQLException("Unable to initialize driver properties due to " + iae.toString());
            }
        }
        postInitialization();
    }

    private void postInitialization() throws SQLException {
        this.tabletTypeCache = this.tabletType.getValueAsEnum();
        this.includedFieldsCache = this.includedFields.getValueAsEnum();
        this.includeAllFieldsCache = this.includedFieldsCache == Query.ExecuteOptions.IncludedFields.ALL;
        this.twopcEnabledCache = this.twopcEnabled.getValueAsBoolean();
        this.simpleExecuteTypeCache = this.executeType.getValueAsEnum() == Constants.QueryExecuteType.SIMPLE;
    }

    static DriverPropertyInfo[] exposeAsDriverPropertyInfo(Properties info, int slotsToReserve) throws SQLException {
        return new ConnectionProperties().exposeAsDriverPropertyInfoInternal(info, slotsToReserve);
    }

    protected DriverPropertyInfo[] exposeAsDriverPropertyInfoInternal(Properties info, int slotsToReserve) throws SQLException {
        initializeProperties(info);
        int numProperties = PROPERTY_LIST.size();
        int listSize = numProperties + slotsToReserve;
        DriverPropertyInfo[] driverProperties = new DriverPropertyInfo[listSize];

        for (int i = slotsToReserve; i < listSize; i++) {
            java.lang.reflect.Field propertyField = PROPERTY_LIST.get(i - slotsToReserve);
            try {
                ConnectionProperty propToExpose = (ConnectionProperty) propertyField.get(this);
                if (info != null) {
                    propToExpose.initializeFrom(info);
                }
                driverProperties[i] = propToExpose.getAsDriverPropertyInfo();
            } catch (IllegalAccessException iae) {
                throw new SQLException("Internal properties failure", iae);
            }
        }
        return driverProperties;
    }

    public Query.ExecuteOptions.IncludedFields getIncludedFields() {
        return this.includedFieldsCache;
    }

    public boolean isIncludeAllFields() {
        return this.includeAllFieldsCache;
    }

    public void setIncludedFields(Query.ExecuteOptions.IncludedFields includedFields) {
        this.includedFields.setValue(includedFields);
        this.includedFieldsCache = includedFields;
        this.includeAllFieldsCache = includedFields == Query.ExecuteOptions.IncludedFields.ALL;
    }

    public boolean getTwopcEnabled() {
        return twopcEnabledCache;
    }

    public void setTwopcEnabled(boolean twopcEnabled) {
        this.twopcEnabled.setValue(twopcEnabled);
        this.twopcEnabledCache = this.twopcEnabled.getValueAsBoolean();
    }

    public Constants.QueryExecuteType getExecuteType() {
        return executeType.getValueAsEnum();
    }

    public boolean isSimpleExecute() {
        return simpleExecuteTypeCache;
    }

    public void setExecuteType(Constants.QueryExecuteType executeType) {
        this.executeType.setValue(executeType);
        this.simpleExecuteTypeCache = this.executeType.getValueAsEnum() == Constants.QueryExecuteType.SIMPLE;
    }

    public Topodata.TabletType getTabletType() {
        return tabletTypeCache;
    }

    public void setTabletType(Topodata.TabletType tabletType) {
        this.tabletType.setValue(tabletType);
        this.tabletTypeCache = this.tabletType.getValueAsEnum();
    }

    abstract static class ConnectionProperty {

        private final String name;
        private final boolean required = false;
        private final String description;
        final Object defaultValue;
        Object valueAsObject;

        private ConnectionProperty(String name, String description, Object defaultValue) {
            this.name = name;
            this.description = description;
            this.defaultValue = defaultValue;
        }

        void initializeFrom(Properties extractFrom) {
            String extractedValue = extractFrom.getProperty(getPropertyName());
            String[] allowable = getAllowableValues();
            if (allowable != null && extractedValue != null) {
                boolean found = false;
                for (String value : allowable) {
                    found |= value.equalsIgnoreCase(extractedValue);
                }
                if (!found) {
                    throw new IllegalArgumentException("Property '" + name + "' Value '" + extractedValue + "' not in the list of allowable values: " + Arrays.toString(allowable));
                }
            }
            extractFrom.remove(getPropertyName());
            initializeFrom(extractedValue);
        }

        abstract void initializeFrom(String extractedValue);

        abstract String[] getAllowableValues();

        public String getPropertyName() {
            return name;
        }

        DriverPropertyInfo getAsDriverPropertyInfo() {
            DriverPropertyInfo dpi = new DriverPropertyInfo(this.name, null);
            dpi.choices = getAllowableValues();
            dpi.value = (this.valueAsObject != null) ? this.valueAsObject.toString() : null;
            dpi.required = this.required;
            dpi.description = this.description;
            return dpi;
        }
    }

    private static class BooleanConnectionProperty extends ConnectionProperty {

        private BooleanConnectionProperty(String name, String description, Boolean defaultValue) {
            super(name, description, defaultValue);
        }

        @Override
        void initializeFrom(String extractedValue) {
            if (extractedValue != null) {
                setValue(extractedValue.equalsIgnoreCase("TRUE") || extractedValue.equalsIgnoreCase("YES"));
            } else {
                this.valueAsObject = this.defaultValue;
            }
        }

        public void setValue(boolean value) {
            this.valueAsObject = value;
        }

        @Override
        String[] getAllowableValues() {
            return new String[]{Boolean.toString(true), Boolean.toString(false), "yes", "no"};
        }

        boolean getValueAsBoolean() {
            return (boolean) valueAsObject;
        }
    }

    private static class StringConnectionProperty extends ConnectionProperty {

        private final String[] allowableValues;

        private StringConnectionProperty(String name, String description, String defaultValue, String[] allowableValuesToSet) {
            super(name, description, defaultValue);
            allowableValues = allowableValuesToSet;
        }

        @Override
        void initializeFrom(String extractedValue) {
            if (extractedValue != null) {
                setValue(extractedValue);
            } else {
                this.valueAsObject = this.defaultValue;
            }
        }

        @Override
        String[] getAllowableValues() {
            return allowableValues;
        }

        public void setValue(String value) {
            this.valueAsObject = value;
        }
    }

    private static class EnumConnectionProperty<T extends Enum<T>> extends ConnectionProperty {

        private final Class<T> clazz;

        private EnumConnectionProperty(String name, String description, Enum<T> defaultValue) {
            super(name, description, defaultValue);
            this.clazz = defaultValue.getDeclaringClass();
            if (!clazz.isEnum()) {
                throw new IllegalArgumentException("EnumConnectionProperty types should be enums");
            }
        }

        @Override
        void initializeFrom(String extractedValue) {
            if (extractedValue != null) {
                setValue(Enum.valueOf(clazz, extractedValue.toUpperCase()));
            } else {
                this.valueAsObject = this.defaultValue;
            }
        }

        public void setValue(T value) {
            this.valueAsObject = value;
        }

        @Override
        String[] getAllowableValues() {
            T[] enumConstants = clazz.getEnumConstants();
            String[] allowed = new String[enumConstants.length];
            for (int i = 0; i < enumConstants.length; i++) {
                allowed[i] = enumConstants[i].toString();
            }
            return allowed;
        }

        T getValueAsEnum() {
            return (T) valueAsObject;
        }
    }
}

import groovy.sql.Sql
import groovy.transform.Canonical
import groovy.transform.ToString

import java.nio.file.Paths
import java.sql.ResultSet
import java.time.LocalDateTime

@GrabConfig(systemClassLoader = true)
@Grab(group = 'com.filemaker',module = 'fmjdbc',version = 'current')
@Grab(group = 'com.oracle',module = 'ojdbc8',version = 'current')

/**
 * Query filemaker database and return Area Location records
 */
List<AreaLocation> areaLocations = []
Sql.withInstance(filemakerDatabaseProps()) {sql ->
    sql.query("""select Serial, CreationTimestamp, CreatedBy, ModificationTimestamp, ModifiedBy, AreaName, AreaActive, "Physical Address", AreaSerial from AREA_Locations""") { ResultSet rs ->
        while (rs.next()) {
            AreaLocation areaLocation = new AreaLocation(
                    rs.getString('Serial'),
                    rs.getTimestamp('CreationTimestamp')?.toLocalDateTime(),
                    rs.getString('CreatedBy'),
                    rs.getTimestamp('ModificationTimestamp')?.toLocalDateTime(),
                    rs.getString('ModifiedBy'),
                    rs.getString('AreaName'),
                    rs.getString('AreaActive'),
                    rs.getString('Physical Address')
            )

            String areaSerial = null
            try {
                areaSerial = rs.getString('AreaSerial')
                areaLocation.setAreaSerial(areaSerial.toBigDecimal())
            } catch (NumberFormatException nfe) {
                System.err.println('Error on AreaSerial field conversion to number ' + areaSerial + ' ' + areaLocation.serial + ' ' + areaLocation.areaName)
            } catch (Exception e) {
                System.err.println('Error on AreaSerial field ' + e.message + ' ' + areaSerial + ' ' + areaLocation.serial + ' ' + areaLocation.areaName)
            }

            areaLocations.add(areaLocation)
        }
    }
}

/**
 * Connect to the Oracle database to insr or update filemaker area location data in custom Oracle table.
 * This will query the Oracle Table to check for the existence of the area location serial
 * If that area location record already exists it will be compared to the filemaker area location record and if different it will be updated
 * If that area location does not already exist it will be inserted into the Oracle table
 * @author Michael Stockman
 */
Sql.withInstance(oracleDatabaseProps()) { sql ->
    areaLocations.each { fmAreaLocation ->
        AreaLocation orclAreaLocation = getAreaLocationBySerial(sql,fmAreaLocation.serial)

        if (orclAreaLocation != null) {
            if (!orclAreaLocation.equals(fmAreaLocation)) {
                updateAreaLocation(sql,fmAreaLocation)
            }
        } else {
            insertAreaLocation(sql,fmAreaLocation)
        }
    }
}

/**
 * Get Area Location record from Oracle table by serial id
 * @author Michael Stockman
 * @param sql
 * @param serial
 * @return
 */
AreaLocation getAreaLocationBySerial(Sql sql, String serial) {
    AreaLocation areaLocation = null
    sql.query("""select syraloc_serial
                           ,syraloc_create_timestamp
                           ,syraloc_created_by
                           ,syraloc_mod_timestamp
                           ,syraloc_modified_by
                           ,syraloc_area_name
                           ,syraloc_area_active
                           ,syraloc_physical_address
                           ,syraloc_area_serial
                       from syraloc
                      where syraloc_serial = ?""",[serial]) { ResultSet rs ->
        if (rs.next()) {
            areaLocation = new AreaLocation(
                    rs.getString('syraloc_serial'),
                    rs.getDate('syraloc_create_timestamp')?.toLocalDateTime(),
                    rs.getString('syraloc_created_by'),
                    rs.getDate('syraloc_mod_timestamp')?.toLocalDateTime(),
                    rs.getString('syraloc_modified_by'),
                    rs.getString('syraloc_area_name'),
                    rs.getString('syraloc_area_active'),
                    rs.getString('syraloc_physical_address'),
                    rs.getBigDecimal('syraloc_area_serial')
            )
        }
    }
    return areaLocation
}

/**
 * Insert Area Location from filemaker into oracle
 * @param sql
 * @param areaLocation
 */
void insertAreaLocation(Sql sql, AreaLocation areaLocation) {
    sql.execute("""insert into syraloc(syraloc_serial
                                          ,syraloc_create_timestamp
                                          ,syraloc_created_by
                                          ,syraloc_mod_timestamp
                                          ,syraloc_modified_by
                                          ,syraloc_area_name
                                          ,syraloc_area_active
                                          ,syraloc_physical_address
                                          ,syraloc_area_serial)
                                    values(?,?,?,?,?,?,?,?,?)"""
            ,[areaLocation.serial,areaLocation.creationTimestamp,areaLocation.createdBy,
              areaLocation.modificationTimestamp,areaLocation.modifiedBy,
              areaLocation.areaName,areaLocation.areaActive,areaLocation.physicalAddress,areaLocation.areaSerial])
}

/**
 * Update area location from filemaker into oracle matched on serial
 * @param sql
 * @param areaLocation
 */
void updateAreaLocation(Sql sql, AreaLocation areaLocation) {
    sql.execute("""update syraloc_create_timestamp = ?
                             ,syraloc_created_by = ?
                             ,syraloc_mod_timestamp = ?
                             ,syraloc_modified_by = ?
                             ,syraloc_area_name = ?
                             ,syraloc_area_active = ?
                             ,syraloc_physical_address = ?
                             ,syraloc_area_serial = ?
                          set syraloc
                        where syraloc_serial = ?
                    """
            ,[areaLocation.creationTimestamp,areaLocation.createdBy,areaLocation.modificationTimestamp,areaLocation.modifiedBy,
              areaLocation.areaName,areaLocation.areaActive,areaLocation.physicalAddress,areaLocation.areaSerial,
              areaLocation.serial])
}

/**
 * Properties object containing filemaker database credentials
 * @return
 */
Properties filemakerDatabaseProps() {
    Properties properties = new Properties()
    Paths.get(System.properties.'user.home','.credentials','filemaker.properties').withInputStream {
        properties.load(it)
    }
    return properties
}

/**
 * Properties object containing oracle database credentials
 * @return
 */
Properties oracleDatabaseProps() {
    Properties properties = new Properties()
    Paths.get(System.properties.'user.home','.credentials','bannerProduction.properties').withInputStream {
        properties.load(it)
    }
    return properties
}

/**
 * Class to represent AreaLocation row from filemaker
 */
@Canonical
@ToString(excludes = "physicalAddress")
class AreaLocation {
    String serial
    LocalDateTime creationTimestamp
    String createdBy
    LocalDateTime modificationTimestamp
    String modifiedBy
    String areaName
    String areaActive
    String physicalAddress
    BigDecimal areaSerial
}



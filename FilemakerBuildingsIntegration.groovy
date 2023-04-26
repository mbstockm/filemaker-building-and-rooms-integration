import groovy.sql.Sql
import groovy.transform.Canonical

import java.nio.file.Paths
import java.sql.ResultSet
import java.time.LocalDateTime

@GrabConfig(systemClassLoader = true)
@Grab(group = 'com.filemaker',module = 'fmjdbc',version = 'current')
@Grab(group = 'com.oracle',module = 'ojdbc8',version = 'current')

/**
 * Query filemaker database and return building records
 * @author Michael Stockman
 */
List<Building> buildings = []
Sql.withInstance(filemakerDatabaseProps()) { sql ->
    sql.query("""SELECT Serial, BuildingRID, Common_Building_Name, ShortName, CreateTimestamp, CreatedBy, Mod_Timestamp, ModifiedBy, EmsID, BannerCode, AreaSerial, BuildingActive  FROM BUI_DCI""") { ResultSet rs ->
        while (rs.next()) {
            Building building = new Building(
                    rs.getBigDecimal('Serial'),
                    rs.getBigDecimal('BuildingRID'),
                    rs.getString('Common_Building_Name'),
                    rs.getString('ShortName'),
                    rs.getTimestamp('CreateTimestamp')?.toLocalDateTime(),
                    rs.getString('CreatedBy'),
                    rs.getTimestamp('Mod_Timestamp')?.toLocalDateTime(),
                    rs.getString('ModifiedBy'),
                    rs.getString('EmsID')?.toBigInteger(),
                    rs.getString('BannerCode'),
                    null,
                    rs.getString('BuildingActive')
            )

            String areaSerial = null
            try {
                areaSerial = rs.getString('AreaSerial');
                building.setAreaSerial(areaSerial?.toBigDecimal())
            } catch (NumberFormatException nfe) {
                System.err.println('Error on Area Serial field conversion to number ' + areaSerial)
            } catch (Exception e) {
                System.err.println('Error on Area Serial field ' + areaSerial + e.getMessage())
            }

            buildings.add(building)
        }
    }
}

/**
 * Connect to Oracle database to insert or update filemaker building data in custom Oracle table.
 * This will query the Oracle table to check for the existence of building serial id#
 * If that building record already exists it will be compared to the filemaker building object and if different it will be updated
 * If that building record doesn't currently exist it will be inserted
 * @author Michael Stockman
 */
Sql.withInstance(oracleDatabaseProps()) { sql ->
    buildings.each {fmBuilding ->
        Building orclBuilding = getBuildingBySerial(sql,fmBuilding.serial)

        if (orclBuilding != null) {
            if (!orclBuilding.equals(fmBuilding)) {
                updateBuilding(sql,fmBuilding)
            }
        } else {
            insertBuilding(sql,fmBuilding)
        }
    }
}

/**
 * Get Building record from Oracle table by serial id#
 * Note the column names are different than the filemaker database
 * @param sql
 * @param serial
 * @return
 */
Building getBuildingBySerial(Sql sql, BigDecimal serial) {
    Building building = null
    sql.query("""SELECT syrbldg_serial
                           ,syrbldg_building_rid
                           ,syrbldg_common_building_name
                           ,syrbldg_short_name
                           ,syrbldg_create_timestamp
                           ,syrbldg_created_by
                           ,syrbldg_mod_timestamp
                           ,syrbldg_modified_by
                           ,syrbldg_ems_id
                           ,syrbldg_banner_code
                           ,syrbldg_area_serial
                           ,syrbldg_building_active
                       FROM syrbldg 
                      WHERE syrbldg_serial = ?""",[serial]) { ResultSet rs ->
        if (rs.next()) {
            building = new Building(
                    rs.getBigDecimal('syrbldg_serial'),
                    rs.getBigDecimal('syrbldg_building_rid'),
                    rs.getString('syrbldg_common_building_name'),
                    rs.getString('syrbldg_short_name'),
                    rs.getTimestamp('syrbldg_create_timestamp')?.toLocalDateTime(),
                    rs.getString('syrbldg_created_by'),
                    rs.getTimestamp('syrbldg_mod_timestamp')?.toLocalDateTime(),
                    rs.getString('syrbldg_modified_by'),
                    (BigInteger) rs.getObject('syrbldg_ems_id'),
                    rs.getString('syrbldg_banner_code'),
                    rs.getBigDecimal('syrbldg_area_serial'),
                    rs.getString('syrbldg_building_active')
            )
        }
    }
    return building
}

/**
 * Insert building from filemaker into oracle
 * @param sql
 * @param building
 */
void insertBuilding(Sql sql, Building building) {
    sql.execute("""insert into syrbldg(syrbldg_serial
                                          ,syrbldg_building_rid
                                          ,syrbldg_common_building_name
                                          ,syrbldg_short_name
                                          ,syrbldg_create_timestamp
                                          ,syrbldg_created_by
                                          ,syrbldg_mod_timestamp
                                          ,syrbldg_modified_by
                                          ,syrbldg_ems_id
                                          ,syrbldg_banner_code
                                          ,syrbldg_area_serial
                                          ,syrbldg_building_active) 
                                   values (?,?,?,?,?,?,?,?,?,?)"""
            ,[building.serial,building.buildingRID,building.commonBuildingName,building.shortName,
              building.createTimestamp,building.createdBy,building.modTimestamp,building.modifiedBy,
              building.emsID,building.bannerCode,building.areaSerial,building.buildingActive])
}

/**
 * Update building by serial id# from filemaker into oracle
 * @param sql
 * @param building
 */
void updateBuilding(Sql sql, Building building){
    sql.execute("""update syrbldg
                          set syrbldg_building_rid = ?
                             ,syrbldg_common_building_name = ?
                             ,syrbldg_short_name = ?
                             ,syrbldg_create_timestamp = ?
                             ,syrbldg_created_by = ?
                             ,syrbldg_mod_timestamp = ?
                             ,syrbldg_modified_by = ?
                             ,syrbldg_ems_id = ?
                             ,syrbldg_banner_code = ?
                             ,syrbldg_area_serial = ?
                             ,syrbldg_building_active = ?
                        where syrbldg_serial = ?"""
            ,[building.buildingRID,building.commonBuildingName,building.shortName,
              building.createTimestamp,building.createdBy,building.modTimestamp,building.modifiedBy,
              building.emsID,building.bannerCode,building.areaSerial,building.buildingActive,building.serial])
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
 * Pojo class to represent a filemaker building
 */
@Canonical
class Building {
    BigDecimal serial
    BigDecimal buildingRID
    String commonBuildingName
    String shortName
    LocalDateTime createTimestamp
    String createdBy
    LocalDateTime modTimestamp
    String modifiedBy
    BigInteger emsID
    String bannerCode
    BigDecimal areaSerial
    String buildingActive
}

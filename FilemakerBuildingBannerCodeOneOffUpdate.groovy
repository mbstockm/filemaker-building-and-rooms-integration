import groovy.sql.Sql
import groovy.transform.Canonical

import java.nio.file.Paths
import java.sql.ResultSet
import java.time.LocalDateTime

@GrabConfig(systemClassLoader = true)
@Grab(group = 'com.filemaker',module = 'fmjdbc',version = 'current')

def buildingSerial = 259
def bannerCode = 'NE1467'
Sql.withInstance(filemakerDatabaseProps()) { sql ->

    Building buildingOriginal = queryBuilding(sql,buildingSerial)
    println "Filemaker Building Before Update"
    println buildingOriginal

    println "\nExecuting Update on Filemaker Database to set Building Code\n"
    updateBannerCode(sql,buildingSerial,bannerCode)

    Building buildingAfter= queryBuilding(sql,buildingSerial)
    println "Filemaker Building After Update"
    println buildingAfter


}

/**
 * Update Filemaker building by serial with a Banner Validation Code
 * @param sql
 * @param serial
 * @param bannerCode
 */
void updateBannerCode(Sql sql,int serial, String bannerCode) {
    sql.execute("update BUI_DCI set BannerCode = ? where Serial = ?",[bannerCode,serial])
}

/**
 * Return building object by serial from filemaker database
 * @param sql
 * @param serial
 * @return
 */
Building queryBuilding(Sql sql, int serial) {
    Building building;
    sql.query("""SELECT Serial, BuildingRID, Common_Building_Name, ShortName, CreateTimestamp, CreatedBy, Mod_Timestamp, ModifiedBy, EmsID, BannerCode  FROM BUI_DCI WHERE Serial = ?""",[serial]) { ResultSet rs ->
        while (rs.next()) {
            building = new Building(
                    rs.getBigDecimal('Serial'),
                    rs.getBigDecimal('BuildingRID'),
                    rs.getString('Common_Building_Name'),
                    rs.getString('ShortName'),
                    rs.getTimestamp('CreateTimestamp')?.toLocalDateTime(),
                    rs.getString('CreatedBy'),
                    rs.getTimestamp('Mod_Timestamp')?.toLocalDateTime(),
                    rs.getString('ModifiedBy'),
                    rs.getString('EmsID')?.toBigInteger(),
                    rs.getString('BannerCode')
            )
        }
    }
    return building;
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
}


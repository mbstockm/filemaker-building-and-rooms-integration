import groovy.sql.Sql
import groovy.transform.Canonical

import java.nio.file.Paths
import java.sql.ResultSet
import java.time.LocalDate


@GrabConfig(systemClassLoader = true)
@Grab(group = 'com.filemaker',module = 'fmjdbc',version = 'current')

def roomSerial = 803
def bannerRoomBuilding = 'THURST'
def bannerRoomNumber = 'T203'
Sql.withInstance(filemakerDatabaseProps()) { sql ->

    Room roomOriginal = queryRoom(sql,roomSerial)
    println "Filemaker Room Before Update"
    println roomOriginal


    println "\nExecuting Update on Filemaker Database to set Building Code\n"
    updateBannerBldgAndRoom(sql,roomSerial,bannerRoomBuilding,bannerRoomNumber)

    Room roomAfter = queryRoom(sql,roomSerial)
    println "Filemaker Room After Update"
    println roomAfter

}

/**
 * Update Filemaker room by serial with a Banner Validation Building Code And Room Number
 * @param sql
 * @param serial
 * @param bannerRoomBuilding
 * @param bannerRoomNumber
 */
void updateBannerBldgAndRoom(Sql sql,int serial, String bannerRoomBuilding, String bannerRoomNumber) {
    sql.execute("update ROO_Room_DCIonSerial set banner_room_bldg= ?, banner_room_number = ? where Serial = ?",[bannerRoomBuilding,bannerRoomNumber,serial])
}

/**
 * Return room object by serial from filemaker database
 * @param sql
 * @param serial
 * @return
 */
Room queryRoom(Sql sql, int serial) {
    Room room
    sql.query("""SELECT Serial, ID, FullName, ShortName, BuildingSerial, Building, RoomSize, RoomType, createdDate, Createdby, ModDate, ModBy, EMSRoomID, banner_room_bldg, banner_room_number FROM ROO_Room_DCIonSerial WHERE Serial = ?""",[serial]) { ResultSet rs ->
        while (rs.next()) {

            room = new Room(
                    rs.getBigDecimal('Serial'),
                    rs.getString('ID')?.trim(),
                    rs.getString('FullName')?.trim(),
                    rs.getString('ShortName')?.trim(),
                    null,
                    rs.getString('Building')?.trim(),
                    null,
                    rs.getString('RoomType')?.trim(),
                    rs.getDate('createdDate')?.toLocalDate(),
                    rs.getString('Createdby'),
                    rs.getDate('ModDate')?.toLocalDate(),
                    rs.getString('ModBy')?.trim(),
                    null,
                    rs.getString('banner_room_bldg')?.trim(),
                    rs.getString('banner_room_number')?.trim()
            )

            // The following fields need error handling so set them after the creation of the object
            String buildingSerial = null
            try {
                buildingSerial = rs.getString('BuildingSerial')
                room.setBuildingSerial(buildingSerial?.toBigDecimal())
            } catch (NumberFormatException nfe) {
                System.err.println('Error on BuildingSerial field conversion to number ' + buildingSerial + ' ' + room)
            } catch (Exception e) {
                System.err.println('Error on BuildingSerial field ' + e.message + ' ' + buildingSerial + ' ' + room)
            }

            String roomSize = null
            try {
                roomSize = rs.getString('RoomSize')
                room.setRoomSize(roomSize?.toBigInteger())
            } catch (NumberFormatException nfe) {
                System.err.println('Error on RoomSize field conversion to number ' + roomSize + ' ' + room + ' ' + nfe.message)
            } catch (Exception e) {
                System.err.println('Error on RoomSize field ' + roomSize + ' ' + room + ' ' + e.message)
            }

            String emsRoomID = null
            try {
                emsRoomID = rs.getString('EMSRoomID')
                room.setEmsRoomID(emsRoomID?.toBigInteger())
            } catch (NumberFormatException nfe) {
                System.err.println('Error on EMSRoomID field conversion to number ' + emsRoomID + ' ' + room + ' ' + nfe.message)
            } catch (Exception e) {
                System.err.println('Error on EMSRoomID field ' + emsRoomID + ' ' + room + ' ' + e.message)
            }
        }
    }
    return room
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
 * Pojo class to represent a filemaker room
 */
@Canonical
class Room {
    BigDecimal serial
    String ID
    String fullName
    String shortName
    BigDecimal buildingSerial
    String building
    BigInteger roomSize
    String roomType
    LocalDate createdDate
    String createdBy
    LocalDate modDate
    String modBy
    BigInteger emsRoomID
    String bannerRoomBuilding
    String bannerRoomNumber
}

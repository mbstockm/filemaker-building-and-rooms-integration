import groovy.sql.Sql
import groovy.transform.Canonical

import java.nio.file.Paths
import java.sql.ResultSet
import java.time.LocalDate

@GrabConfig(systemClassLoader = true)
@Grab(group = 'com.filemaker',module = 'fmjdbc',version = 'current')
@Grab(group = 'com.oracle',module = 'ojdbc8',version = 'current')

/**
 * Query filemaker database and return room records
 * @author Michael Stockman
 */
List<Room> rooms = []
Sql.withInstance(filemakerDatabaseProps()) { sql ->
    sql.query("""SELECT Serial, ID, FullName, ShortName, BuildingSerial, Building, RoomSize, RoomType, createdDate, Createdby, ModDate, ModBy, EMSRoomID, banner_room_bldg, banner_room_number FROM ROO_Room_DCIonSerial""") { ResultSet rs ->
        while (rs.next()) {
            Room room = new Room(
                    rs.getBigDecimal('Serial'),
                    rs.getString('ID')?.trim(),
                    rs.getString('FullName')?.trim(),
                    rs.getString('ShortName')?.trim(),
                    null,
                    rs.getString('Building')?.trim(),
                    null,
                    rs.getString('RoomType')?.trim(),
                    rs.getDate('createdDate')?.toLocalDate(),
                    rs.getString('Createdby')?.trim(),
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
                emsRoomID= rs.getString('EMSRoomID')
                room.setEmsRoomID(emsRoomID?.toBigInteger())
            } catch (NumberFormatException nfe) {
                System.err.println('Error on EMSRoomID field conversion to number ' + emsRoomID + ' ' + room + ' ' + nfe.message)
            } catch (Exception e) {
                System.err.println('Error on EMSRoomID field ' + emsRoomID + ' ' + room + ' ' + e.message)
            }

            rooms.add(room)
        }
    }
}

/**
 * Connect to Oracle database to insert or update filemaker room data in custom Oracle table.
 * This will query the Oracle table to check for the existence of room serial id#
 * If that room record already exists it will be compared to the filemaker room object and if different it will be updated
 * If that room record doesn't currently exist it will be inserted
 * @author Michael Stockman
 */
Sql.withInstance(oracleDatabaseProps()) { sql ->
    rooms.each {fmRoom ->
        Room orclRoom = getRoomBySerial(sql,fmRoom.serial)

        if (orclRoom != null) {
            if (!orclRoom.equals(fmRoom)) {
                updateRoom(sql,fmRoom)
            }
        } else {
            insertRoom(sql,fmRoom)
        }
    }
}

/**
 * Get Room record from Oracle table by serial id#
 * Note the column names are different than the filemaker database
 * @param sql
 * @param serial
 * @return
 */
Room getRoomBySerial(Sql sql, BigDecimal serial) {
    Room room = null
    sql.query("""SELECT syrroom_serial
                           ,syrroom_id
                           ,syrroom_full_name
                           ,syrroom_short_name
                           ,syrroom_building_serial
                           ,syrroom_building
                           ,syrroom_room_size
                           ,syrroom_room_type
                           ,syrroom_created_date
                           ,syrroom_created_by
                           ,syrroom_mod_date
                           ,syrroom_mod_by
                           ,syrroom_ems_room_id
                           ,syrroom_banner_room_bldg
                           ,syrroom_banner_room_number
                       FROM syrroom
                      WHERE syrroom_serial = ?""",[serial]) { ResultSet rs ->
        if (rs.next()) {
            room = new Room(
                    rs.getBigDecimal('syrroom_serial'),
                    rs.getString('syrroom_id'),
                    rs.getString('syrroom_full_name'),
                    rs.getString('syrroom_short_name'),
                    rs.getBigDecimal('syrroom_building_serial'),
                    rs.getString('syrroom_building'),
                    (BigInteger) rs.getObject('syrroom_room_size'),
                    rs.getString('syrroom_room_type'),
                    rs.getDate('syrroom_created_date')?.toLocalDate(),
                    rs.getString('syrroom_created_by'),
                    rs.getDate('syrroom_mod_date')?.toLocalDate(),
                    rs.getString('syrroom_mod_by'),
                    (BigInteger) rs.getObject('syrroom_ems_room_id'),
                    rs.getString('syrroom_banner_room_bldg'),
                    rs.getString('syrroom_banner_room_number')
            )
        }
    }
    return room
}

/**
 * Insert room from filemaker into oracle
 * @param sql
 * @param room
 */
void insertRoom(Sql sql, Room room) {
    sql.execute("""insert into syrroom(syrroom_serial
                                          ,syrroom_id
                                          ,syrroom_full_name
                                          ,syrroom_short_name
                                          ,syrroom_building_serial
                                          ,syrroom_building
                                          ,syrroom_room_size
                                          ,syrroom_room_type
                                          ,syrroom_created_date
                                          ,syrroom_created_by
                                          ,syrroom_mod_date
                                          ,syrroom_mod_by
                                          ,syrroom_ems_room_id
                                          ,syrroom_banner_room_bldg
                                          ,syrroom_banner_room_number) 
                                   values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
            ,[room.serial,room.ID,room.fullName,room.shortName,room.buildingSerial,room.building,room.roomSize,room.roomType,
              room.createdDate,room.createdBy,room.modDate,room.modBy,
              room.emsRoomID,room.bannerRoomBuilding,room.bannerRoomNumber])
}

/**
 * Update room by serial id# from filemaker into oracle
 * @param sql
 * @param room
 */
void updateRoom(Sql sql, Room room){
    sql.execute("""update syrroom
                          set syrroom_id = ?
                             ,syrroom_full_name = ?
                             ,syrroom_short_name = ?
                             ,syrroom_building_serial = ?
                             ,syrroom_building = ?
                             ,syrroom_room_size = ?
                             ,syrroom_room_type = ?
                             ,syrroom_created_date = ?
                             ,syrroom_created_by = ?
                             ,syrroom_mod_date = ?
                             ,syrroom_mod_by = ?
                             ,syrroom_ems_room_id = ?
                             ,syrroom_banner_room_bldg = ?
                             ,syrroom_banner_room_number = ?
                        where syrroom_serial = ?"""
            ,[room.ID,room.fullName,room.shortName,room.buildingSerial,room.building,room.roomSize,room.roomType,
              room.createdDate,room.createdBy,room.modDate,room.modBy,
              room.emsRoomID,room.bannerRoomBuilding,room.bannerRoomNumber,room.serial])
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

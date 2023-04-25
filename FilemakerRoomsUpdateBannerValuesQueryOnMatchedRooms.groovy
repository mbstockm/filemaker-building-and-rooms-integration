import groovy.sql.Sql
import groovy.transform.Canonical

import java.nio.file.Paths
import java.sql.ResultSet
import java.time.LocalDateTime

@GrabConfig(systemClassLoader = true)
@Grab(group = 'com.filemaker',module = 'fmjdbc',version = 'current')
@Grab(group = 'com.oracle',module = 'ojdbc8',version = 'current')

def matchedRooms = []
Sql.withInstance(oracleDatabaseProps()) { sql ->
    matchedRooms = queryMatchedRooms(sql)
}

Sql.withInstance(filemakerDatabaseProps()) { sql ->
    matchedRooms.each { room ->
        println room
//        updateRoomBannerValuesBySerial(sql,room)
    }
}

List<Room> queryMatchedRooms(Sql sql) {
    List<Room> rooms = []
    sql.query("""select syrroom_serial serial, 
       syrroom_building_serial bldg_serial, 
       syrroom_full_name full_name, 
       syrroom_short_name short_name, 
       syrbldg_banner_code bldg_code, 
       stvbldg_desc bldg_desc, 
       count(*) cntrooms,
       max(rd.slbrdef_room_number) rdef_room_number
  from syrbldg,stvbldg,syrroom,slbrdef rd
 where syrbldg_serial = syrroom_building_serial 
   and syrbldg_banner_code = stvbldg_code
   and rd.slbrdef_bldg_code = stvbldg_code
   and upper(syrroom.syrroom_short_name) like '%'||rd.slbrdef_room_number
   and rd.slbrdef_term_code_eff = (select max(rd2.slbrdef_term_code_eff) from slbrdef rd2 where rd2.slbrdef_bldg_code = rd.slbrdef_bldg_code and rd2.slbrdef_room_number = rd.slbrdef_room_number)
   and syrroom.syrroom_banner_room_bldg||syrroom.syrroom_banner_room_number is null
   and syrroom.syrroom_serial not in (692)
group by syrroom_serial 
        ,syrroom_building_serial   
        ,syrroom_full_name
        ,syrroom_short_name
        ,syrbldg_banner_code
        ,stvbldg_desc
having count(*) = 1    """) { rs ->
        while (rs.next()) {
            Room room = new Room(
                    rs.getBigDecimal('serial'),
                    rs.getBigDecimal('bldg_serial'),
                    rs.getString('full_name'),
                    rs.getString('short_name'),
                    rs.getString('bldg_code'),
                    rs.getString('bldg_desc'),
                    rs.getInt('cntrooms'),
                    rs.getString('rdef_room_number')
            )
            rooms.add(room)
        }
    }
    return rooms
}

//List<Room> queryMatchedRooms(Sql sql) {
//    List<Room> rooms = []
//    sql.query("""select syrroom_serial serial,
//       syrroom_building_serial bldg_serial,
//       syrroom_full_name full_name,
//       syrroom_short_name short_name,
//       syrbldg_banner_code bldg_code,
//       stvbldg_desc bldg_desc,
//       count(*) cntrooms,
//       max(rd.slbrdef_room_number) rdef_room_number
//  from syrbldg,stvbldg,syrroom,slbrdef rd
// where syrbldg_serial = syrroom_building_serial
//   and syrbldg_banner_code = stvbldg_code
//   and rd.slbrdef_bldg_code = stvbldg_code
//   and upper(syrroom.syrroom_short_name) like '%'||rd.slbrdef_room_number
//   and rd.slbrdef_term_code_eff = (select max(rd2.slbrdef_term_code_eff) from slbrdef rd2 where rd2.slbrdef_bldg_code = rd.slbrdef_bldg_code and rd2.slbrdef_room_number = rd.slbrdef_room_number)
//   and syrroom.syrroom_banner_room_bldg||syrroom.syrroom_banner_room_number is null
//   and syrroom.syrroom_serial not in (692)
//group by syrroom_serial
//        ,syrroom_building_serial
//        ,syrroom_full_name
//        ,syrroom_short_name
//        ,syrbldg_banner_code
//        ,stvbldg_desc
//having count(*) = 1    """) { rs ->
//        while (rs.next()) {
//            Room room = new Room(
//                    rs.getBigDecimal('serial'),
//                    rs.getBigDecimal('bldg_serial'),
//                    rs.getString('full_name'),
//                    rs.getString('short_name'),
//                    rs.getString('bldg_code'),
//                    rs.getString('bldg_desc'),
//                    rs.getInt('cntrooms'),
//                    rs.getString('rdef_room_number')
//            )
//            rooms.add(room)
//        }
//    }
//    return rooms
//}

void updateRoomBannerValuesBySerial(Sql sql, Room room) {
    sql.execute("""update ROO_Room_DCIonSerial set banner_room_bldg = ?, banner_room_number = ? where Serial = ?""",[room.buildingBannerCode,room.bannerRoomNumber,room.serial])
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
 * Pojo class to represent a filemaker room for updating banner codes
 */
@Canonical
class Room {
    BigDecimal serial
    BigDecimal buildingSerial
    String buildingFullName
    String buildingShortName
    String buildingBannerCode
    String buildingBannerDesc
    Integer cntRooms
    String bannerRoomNumber
}


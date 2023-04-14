-- MariaDB dump 10.19  Distrib 10.6.12-MariaDB, for debian-linux-gnu (x86_64)
--
-- Host: localhost    Database: eddb
-- ------------------------------------------------------
-- Server version	10.6.12-MariaDB-0ubuntu0.22.04.1

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


--
-- Table structure for table `spansh_commodity_pricing`
--

DROP TABLE IF EXISTS `spansh_commodity_pricing`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `spansh_commodity_pricing` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `station_id` bigint(20) NOT NULL,
  `commodity_id` int(11) NOT NULL,
  `demand` int(11) DEFAULT NULL,
  `supply` int(11) DEFAULT NULL,
  `buy_price` int(11) DEFAULT NULL,
  `sell_price` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `spansh_station_commodity_unique` (`station_id`,`commodity_id`),
  KEY `commodity_id` (`commodity_id`),
  CONSTRAINT `spansh_commodity_pricing_ibfk_1` FOREIGN KEY (`station_id`) REFERENCES `stations` (`id`),
  CONSTRAINT `spansh_commodity_pricing_ibfk_2` FOREIGN KEY (`commodity_id`) REFERENCES `spansh_commodities` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=8405888 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `spansh_commodity_pricing`
--

LOCK TABLES `spansh_commodity_pricing` WRITE;
/*!40000 ALTER TABLE `spansh_commodity_pricing` DISABLE KEYS */;
--INSERT INTO `spansh_commodity_pricing` VALUES COMMODITY_PRICING_HERE
COMMODITY_PRICING_HERE
/*!40000 ALTER TABLE `spansh_commodity_pricing` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `spansh_modules_sold`
--

DROP TABLE IF EXISTS `spansh_modules_sold`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `spansh_modules_sold` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `station_id` bigint(20) NOT NULL,
  `module_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `spansh_station_module_unique` (`station_id`,`module_id`),
  KEY `module_id` (`module_id`),
  CONSTRAINT `spansh_modules_sold_ibfk_1` FOREIGN KEY (`station_id`) REFERENCES `stations` (`id`),
  CONSTRAINT `spansh_modules_sold_ibfk_2` FOREIGN KEY (`module_id`) REFERENCES `spansh_modules` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=8017939 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `spansh_modules_sold`
--

LOCK TABLES `spansh_modules_sold` WRITE;
/*!40000 ALTER TABLE `spansh_modules_sold` DISABLE KEYS */;
-- INSERT INTO `spansh_modules_sold` VALUES ((1, 2, 3));
MODULES_SOLD_HERE
/*!40000 ALTER TABLE `spansh_modules_sold` ENABLE KEYS */;
UNLOCK TABLES;

/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2023-04-14 14:15:35

-- phpMyAdmin SQL Dump
-- version 3.2.4
-- http://www.phpmyadmin.net
--
-- Servidor: localhost
-- Tiempo de generación: 15-04-2013 a las 16:14:03
-- Versión del servidor: 5.1.37
-- Versión de PHP: 5.2.11

SET SQL_MODE="NO_AUTO_VALUE_ON_ZERO";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;

--
-- Base de datos: `HADOOP`
--

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `score`
--

CREATE TABLE `score` (
  `SCORE_ID` int(11) NOT NULL AUTO_INCREMENT,
  `SCORE_FECHA` date NOT NULL,
  `SCORE_NOMBRE` varchar(60) NOT NULL,
  `SCORE_PUNTOS` int(11) NOT NULL,
  PRIMARY KEY (`SCORE_ID`)
) ENGINE=MyISAM  DEFAULT CHARSET=latin1 AUTO_INCREMENT=36 ;

--
-- Volcar la base de datos para la tabla `score`
--

INSERT INTO `score` VALUES(1, '2012-11-01', 'Pepe Perez Gonzalez', 21);
INSERT INTO `score` VALUES(2, '2012-11-01', 'Ana Lopez Fernandez', 14);
INSERT INTO `score` VALUES(3, '2012-11-01', 'Maria Garcia Martinez', 11);
INSERT INTO `score` VALUES(4, '2012-11-01', 'Pablo Sanchez Rodriguez', 9);
INSERT INTO `score` VALUES(5, '2012-11-01', 'Angel Martin Hernandez', 3);
INSERT INTO `score` VALUES(6, '2012-11-15', 'Pepe Perez Gonzalez', 22);
INSERT INTO `score` VALUES(7, '2012-11-15', 'Maria Garcia Martinez', 15);
INSERT INTO `score` VALUES(8, '2012-11-15', 'Cristina Ruiz Gomez', 23);
INSERT INTO `score` VALUES(9, '2012-12-01', 'Pepe Perez Gonzalez', 25);
INSERT INTO `score` VALUES(10, '2012-12-01', 'Ana Lopez Fernandez', 15);
INSERT INTO `score` VALUES(11, '2012-12-01', 'Pablo Sanchez Rodriguez', 8);
INSERT INTO `score` VALUES(12, '2012-12-01', 'Maria Garcia Martinez', 32);
INSERT INTO `score` VALUES(13, '2012-12-15', 'Maria Garcia Martinez', 47);
INSERT INTO `score` VALUES(14, '2012-12-15', 'Pepe Perez Gonzalez', 13);
INSERT INTO `score` VALUES(15, '2012-12-15', 'Cristina Ruiz Gomez', 3);
INSERT INTO `score` VALUES(16, '2012-12-15', 'Angel Martin Hernandez', 13);
INSERT INTO `score` VALUES(17, '2013-01-01', 'Ana Lopez Fernandez', 5);
INSERT INTO `score` VALUES(18, '2013-01-01', 'Pablo Sanchez Rodriguez', 2);
INSERT INTO `score` VALUES(19, '2013-01-01', 'Pepe Perez Gonzalez', 17);
INSERT INTO `score` VALUES(20, '2013-01-01', 'Maria Garcia Martinez', 3);
INSERT INTO `score` VALUES(21, '2013-01-01', 'Angel Martin Hernandez', 32);
INSERT INTO `score` VALUES(22, '2013-01-15', 'Pablo Sanchez Rodriguez', 2);
INSERT INTO `score` VALUES(23, '2013-01-15', 'Pepe Perez Gonzalez', 17);
INSERT INTO `score` VALUES(24, '2013-01-15', 'Maria Garcia Martinez', 3);
INSERT INTO `score` VALUES(25, '2013-01-15', 'Angel Martin Hernandez', 32);
INSERT INTO `score` VALUES(26, '2013-02-01', 'Ana Lopez Fernandez', 5);
INSERT INTO `score` VALUES(27, '2013-02-01', 'Maria Garcia Martinez', 32);
INSERT INTO `score` VALUES(28, '2013-02-01', 'Pepe Perez Gonzalez', 17);
INSERT INTO `score` VALUES(29, '2013-02-01', 'Cristina Ruiz Gomez', 19);
INSERT INTO `score` VALUES(30, '2013-02-15', 'Pablo Sanchez Rodriguez', 32);
INSERT INTO `score` VALUES(31, '2013-02-15', 'Pepe Perez Gonzalez', 11);
INSERT INTO `score` VALUES(32, '2013-02-15', 'Maria Garcia Martinez', 13);
INSERT INTO `score` VALUES(33, '2013-02-15', 'Angel Martin Hernandez', 3);
INSERT INTO `score` VALUES(34, '2013-02-15', 'Cristina Ruiz Gomez', 9);
INSERT INTO `score` VALUES(35, '2013-02-15', 'Ana Lopez Fernandez', 51);

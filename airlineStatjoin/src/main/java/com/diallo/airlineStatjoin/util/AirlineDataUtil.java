package com.diallo.airlineStatjoin.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;

public class AirlineDataUtil {
	public static  String[] extraitDetailsCompagnie(Text line) {
		String[] champs = line.toString().split("\",\"");
		champs[0] = champs[0].replaceAll("\"", "");
		champs[1] = champs[1].replaceAll("\"", "");
		return champs;
	}
	
	public static InfosVol parseInfosVolFromText(Text line){
		String champs[] = line.toString().split(",");
		InfosVol  vol = new InfosVol();
		vol.annee.set(Integer.parseInt(getYear(champs)));
		vol.mois.set(Integer.parseInt(getMonth(champs)));
		vol.jour.set(Integer.parseInt(getDayOfMonth(champs)));
		vol.AeroportArrive.set(getDestination(champs));
		vol.AeroportDepart.set(getOrigin(champs));
		vol.Compagnie.set(getUniqueCarrier(champs));
		vol.retardDepart.set(parseMinutes(getDepartureDelay(champs), 0));
		vol.retardArrive.set(parseMinutes(getArrivalDelay(champs), 0));
		if (parseBoolean(getCancelled(champs), false)){
			vol.statutVol.set(InfosVol.CANCELLED);
		}
		else if (parseBoolean(getDiverted(champs), false))
			vol.statutVol.set(InfosVol.DIVERTED);
		else 
			vol.statutVol.set(InfosVol.NORMAL);
		return vol;
	}
	
	public static Text infosVolToText(InfosVol vol){
		StringBuilder sb = new StringBuilder();
		sb.append(vol.annee).append(',');
		sb.append(vol.mois).append(',');
		sb.append(vol.jour).append(',');
		sb.append(vol.AeroportDepart).append(',');
		sb.append(vol.AeroportArrive).append(',');
		sb.append(vol.Compagnie).append(',');
		sb.append(vol.statutVol);
		
		return new Text(sb.toString());
	}
	

	// cette fonction détecte si la ligne passée est la ligne aec les intitulés des colonnes
	public static boolean isHeader(Text ligne) {
		String[] champs = ligne.toString().split(",");
		return (champs.length > 0 && champs[0].equalsIgnoreCase("year"));
	}
	
	
	// ces deux convertisseurs permettent de gérer les erreurs de conversions
	// dans ce cas il renvoie la valeur par defaut, par exemple si la valeur est NA(not available)
	public static int parseMinutes(String minutes, int defaultValue) {
		try { return Integer.parseInt(minutes); }
		catch (Exception ex) { return defaultValue;}
	}

	// de plus, les boolean sont sous forme de 0/1 dans le fichier...donc a convertir avant lecture
	public static boolean parseBoolean(String bool, boolean defaultValue) {
		try { 
			int val = Integer.parseInt(bool);
			return (val == 1);
		}
		catch (Exception ex) { return defaultValue;}
	}
	
	// concatene (join) les champs avec le separateur
	public static StringBuilder mergeStringArray(String[] array, String separator) {
		StringBuilder sb = new StringBuilder();
		if (array != null && array.length > 0) {
			sb.append(array[0]);
			for (int i = 1; i < array.length; i++ ) {
				sb.append(separator).append(array[i]);
			}
		}
		return sb;
	}

	// selection des champs nous intéréssant
	public static String[] getSelectedColumnsA(Text ligne) {
		String[] champsIn = ligne.toString().split(",");
		String[] champsOut = new String[10];
		champsOut[0] = getDate(champsIn);
		champsOut[1] = getDepartureTime(champsIn);
		champsOut[2] = getArrivalTime(champsIn);
		champsOut[3] = getOrigin(champsIn);
		champsOut[4] = getDestination(champsIn);
		champsOut[5] = getDistance(champsIn);
		champsOut[6] = getElapsedTime(champsIn);
		champsOut[7] = getScheduledElapsedTime(champsIn);
		champsOut[8] = getDepartureDelay(champsIn);
		champsOut[9] = getArrivalDelay(champsIn);
		return champsOut;
	}
	
	// construit la date a partir des 3 champs annee, mois et jour 
	public static String getDate(String[] champs) {
		StringBuilder sb = new StringBuilder();
		sb.append(getMonth(champs))
		  .append('/')
		  .append(getDayOfMonth(champs))
		  .append('/')
		  .append(getYear(champs));
		return sb.toString();
	}
	
	public static String getYear(String[] champs) {	return champs[0];}
	// ces deux fonctions renvoie le mois ou le jour  sous la forme "01" ... "12"..."31"
	public static String getMonth(String[] champs) { return StringUtils.leftPad(champs[1], 2, "0"); }
	public static String getDayOfMonth(String[] champs) { return StringUtils.leftPad(champs[2], 2, "0"); }

	// jour de la semaine entre 1 et 7
	public static String getDayOfWeek(String[] champs) { return champs[3]; }
	
	// pour les temps(heure minutes), renvoie sous la forme "0945" ... "1510"...
	public static String getDepartureTime(String[] champs) { return StringUtils.leftPad(champs[4], 4, "0"); }
	public static String getScheduledDepartureTime(String[] champs) { return StringUtils.leftPad(champs[5], 4, "0"); }
	public static String getArrivalTime(String[] champs) { return StringUtils.leftPad(champs[6], 4, "0"); }
	public static String getScheduledArrivalTime(String[] champs) { return StringUtils.leftPad(champs[7], 4, "0"); }
	
	public static String getUniqueCarrier(String[] champs) { return champs[8]; }
	public static String getFlightNum(String[] champs) { return champs[9]; }
	public static String getTailNum(String[] champs) { return champs[10]; }
	public static String getElapsedTime(String[] champs) { return champs[11]; }
	public static String getScheduledElapsedTime(String[] champs) { return champs[12]; }
	public static String getAirTime(String[] champs) { return champs[13]; }
	public static String getArrivalDelay(String[] champs) { return champs[14]; }
	public static String getDepartureDelay(String[] champs) { return champs[15]; }
	public static String getOrigin(String[] champs) { return champs[16]; }
	public static String getDestination(String[] champs) { return champs[17]; }
	public static String getDistance(String[] champs) { return champs[18]; }
	public static String getCancelled(String[] champs) { return champs[21]; }
	public static String getCancellationCode(String[] champs) { return champs[22]; }
	public static String getDiverted(String[] champs) { return champs[23]; }
	public static String getCarrierDelay(String[] champs) { return champs[24]; }
	public static String getWeatherDelay(String[] champs) { return champs[25]; }
	public static String getNasDelay(String[] champs) { return champs[26]; }
	public static String getSecurityDelay(String[] champs) { return champs[27]; }
	public static String getLateAircraftDelay(String[] champs) { return champs[28]; }
	

	
	
}

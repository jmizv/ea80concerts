DROP DATABASE IF EXISTS ea80;
CREATE DATABASE ea80;
SET schema 'ea80';

create table city (
	id bigint primary key,
	name varchar(255) not null,
	country varchar(2) not null default 'DE'
);

create table venue (
	id bigint primary key,
	name varchar(255) not null,
	cityid bigint not null references city(id)
);

create table band (
	id bigint primary key,
	name varchar(255) not null
);

create table concert (
	id bigint primary key,
	dateofconcert date not null,
	dateformat varchar(16) not null default 'dd.MM.yyyy',
	venueid bigint not null references venue(id),
	otherbands bigint[],
	canceled boolean not null default '0',
	price_boxoffice numeric(6,2),
	price_preselling numeric(6,2),
	currency varchar(3) default 'EUR'
);

CREATE VIEW v_concert AS
 SELECT dateofconcert, c.name "city", v.name "venue", otherbands, case when canceled='0' then '' else 'abgesagt' end
 FROM concert k,city c,venue v
 WHERE k.venueid=v.id AND v.cityid=c.id
 ORDER by 1 DESC;



CREATE OR REPLACE PROCEDURE createConcert(VARCHAR(10),VARCHAR(255),VARCHAR(255),VARCHAR(255)[],BOOLEAN,NUMERIC(6,2),NUMERIC(6,2))
LANGUAGE plpgsql
AS $$
DECLARE
 vBandId bigint;
 vVenueId bigint;
 vCityId bigint;
 vConcertId bigint;
 vBandsId bigint[];
 vBandName varchar(255);
BEGIN
SELECT max(id) INTO vCityId FROM city WHERE name=$2;
IF vCityId IS NULL THEN
	SELECT coalesce(max(id),0)+1 INTO vCityId FROM city;
	INSERT INTO city (id,name) VALUES (vCityId,$2);
END IF;

SELECT max(id) INTO vVenueId FROM venue WHERE name=$3 AND cityid=vCityId;
IF vVenueId IS NULL THEN
	SELECT coalesce(max(id),0)+1 INTO vVenueId FROM venue;
	INSERT INTO venue(id,name,cityid) VALUES (vVenueId,$3,vCityId);
END IF;

IF $4 IS NOT NULL THEN
	FOREACH vBandName IN ARRAY $4
	LOOP
		SELECT max(id) INTO vBandId FROM band WHERE name=vBandName;
		IF vBandId IS NULL THEN
			SELECT coalesce(max(id),0)+1 INTO vBandId FROM band;
			INSERT INTO band(id,name) VALUES (vBandId,vBandName);
		END IF;
		vBandsId := array_append(vBandsId,vBandId);
	END LOOP;
END IF;

SELECT coalesce(max(id),0)+1 INTO vConcertId FROM concert;
INSERT INTO concert (id,dateofconcert,venueid,otherbands,canceled,price_boxoffice,price_preselling)
	VALUES (vConcertId,cast($1 as date),vVenueId,vBandsId,$5,$6,$7);

END;
$$
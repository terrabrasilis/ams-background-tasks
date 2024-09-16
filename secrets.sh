mkdir -p secrets

echo "postgresql://ams:postgres@192.168.0.51:5432/AMSCHECK" > secrets/ams_db_url.txt
echo "postgresql://ams:postgres@192.168.0.51:5432/auxiliary" > secrets/ams_aux_db_url.txt
echo "postgresql://ams:postgres@192.168.0.51:5432/raw_active_fires2" > secrets/ams_af_db_url.txt
echo "postgresql://ams:postgres@192.168.0.51:5432/DETER-B" > secrets/ams_amz_deter_b_db_url.txt
echo "postgresql://ams:postgres@192.168.0.51:5432/deter_cerrado_nb" > secrets/ams_cer_deter_b_db_url.txt

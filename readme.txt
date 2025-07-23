python -m venv env1
.\env1\Scripts\activate
source env/bin/activate # for Linux

requirements.txt
pip install -r requirements.txt

uvicorn app:app --reload

netstat -ano | findstr :portNumber
taskkill /PID typeyourPIDhere /F

cloudflared tunnel --url http://localhost:8000

CREATE OR REPLACE VIEW object_phase_v AS
SELECT 
    p.object_id,
    p.phase_num,
    p.name,
    p.description as phase_description,
    p.status as phase_status,
    o.description as object_description,
    o.object_type,
    o.volume,
    o.status AS object_status
FROM public."ObjectPhases" p
INNER JOIN public."ObjectMaster" o ON p.object_id = o.object_id
ORDER BY p.object_id, p.phase_num;        

CREATE OR REPLACE VIEW object_phase_step_v AS
SELECT 
    p.object_id,
    p.phase_num,
    p.step_id,
    p.step_seq,
    p.phase_name,
    s.notebook,
    s.step_type,
    s.description as step_description
FROM public."ObjectsPhasesSteps" p
LEFT OUTER JOIN public."StepMaster" s ON p.step_id = s.step_id
ORDER BY p.object_id, p.phase_num, p.step_seq;        


cf push --var prefixval=dev
cf delete <<appname>> -r
cf domains
cf delete-route <<DOMAIN>> [--hostname HOSTNAME] [--path PATH] [-f]
cf env <<appname>>
cf set-env APP_NAME ENV_VAR_NAME ENV_VAR_VALUE
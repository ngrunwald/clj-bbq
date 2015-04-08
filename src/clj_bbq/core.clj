(ns clj-bbq.core
  (:require [clojure.java.io :as io])
  (:import [com.google.api.client.googleapis.auth.oauth2 GoogleCredential GoogleCredential$Builder]
           [com.google.api.client.googleapis.javanet GoogleNetHttpTransport]
           [com.google.api.services.bigquery.model JobConfigurationQuery JobConfiguration Job]
           ;; [com.google.api.client.http HttpTransport HttpRequestInitializer]
           ;; [com.google.api.client.http.javanet NetHttpTransport]
           ;; [com.google.api.client.json JsonFactory]
           [com.google.api.client.json.jackson JacksonFactory]
           [com.google.api.services.bigquery Bigquery Bigquery$Builder BigqueryScopes]))

(defn- get-credentials
  ([http-transport json-factory key-file service-account-id account-email]
   (-> (GoogleCredential$Builder.)
       (.setTransport http-transport)
       (.setJsonFactory json-factory)
       (.setServiceAccountId service-account-id)
       (.setServiceAccountScopes [BigqueryScopes/BIGQUERY])
       (.setServiceAccountPrivateKeyFromP12File (io/file key-file))
       (cond-> account-email (.setServiceAccountUser account-email))
       (.build)))
  ([http-transport json-factory key-file service-account-id]
   (get-credentials http-transport json-factory key-file service-account-id nil)))

(defn get-bq-service
  ([key-file service-account-id application-name account-email]
   (let [http-transport (GoogleNetHttpTransport/newTrustedTransport)
         json-factory (JacksonFactory.)
         credentials (get-credentials http-transport json-factory key-file service-account-id account-email)]
     (-> (Bigquery$Builder. http-transport json-factory credentials)
         (.setApplicationName application-name)
         (.setHttpRequestInitializer credentials)
         (.build))))
  ([key-file service-account-id application-name]
   (get-bq-service key-file service-account-id application-name nil)))

(defn list-projects
  [service {:keys [size fields lazy?]}]
  (.execute (.list (.projects service))))

(defn list-datasets
  [service project-id {:keys [size fields lazy?]}]
  (.execute (.list (.datasets service) project-id)))

(defn list-tables
  [service project-id dataset-id {:keys [size fields lazy?]}]
  (.execute (.list (.tables service) project-id dataset-id)))

(defn make-job
  [query {:keys [] :as opts}]
  (let [conf-query (-> (JobConfigurationQuery.)
                       (.setQuery query))
        job-conf (-> (JobConfiguration.)
                     (.setQuery conf-query))]
    (-> (Job.)
        (.setConfiguration job-conf))))

(defn run-job
  [service project-id job]
  (-> service
      (.jobs)
      (.insert project-id job)
      (.execute)))

(defn get-job
  [service project-id job-id]
  (-> (.jobs service)
      (.get project-id job-id)
      (.execute)))

(defn row->vec
  [r]
  (persistent!
   (reduce
    (fn [acc f]
      (conj! acc (.getV f)))
    (transient []) (.getF r))))

(defn get-table
  [service project-id dataset-id table-id]
  (let [result (-> service
                   (.tabledata)
                   (.list project-id dataset-id table-id)
                   (.execute))]
    (.getRows result)))

# infrastructure/backend-setup/variables.tf
# ============================================================================
# VARIABLES: Configuración paramétrica del backend
# ============================================================================

# REGIÓN DE AWS
# Especifica en qué región crear todos los recursos
variable "aws_region" {
  description = "Región de AWS donde se desplegará el backend de Terraform"
  type        = string
  default     = "us-east-1"
}

# NOMBRE DEL BUCKET DE ESTADO
# IMPORTANTE: Los buckets S3 son ÚNICOS GLOBALMENTE (no puede haber duplicados)
# Formato recomendado: <proyecto>-<ambiente>-<empresa>-<año>
# Ej: video-pipeline-prod-acme-2026
variable "state_bucket_name" {
  description = "Nombre ÚNICO globalmente para el bucket S3 de estado"
  type        = string
  default     = "hirenovik-tf-state-edgar-alfaro-2026"  
}

variable "lock_table_name" {
  description = "Nombre de la tabla DynamoDB para state locking"
  type        = string
  default     = "hirenovik-tf-locks"  
}

# DURACIÓN DE RETENCIÓN DE VERSIONES (días)
# Cuánto tiempo conservar versiones antiguas del estado para poder hacer rollback
# Recomendación: 90 días = suficiente para debugging pero controla costos
variable "version_retention_days" {
  description = "Días para retener versiones antiguas del estado (rollback)"
  type        = number
  default     = 90
}

# DURACIÓN DE RETENCIÓN DE LOGS (días)
# Cuánto tiempo mantener logs de acceso antes de borrar automáticamente
# Recomendación: 30 días = suficiente auditoría, económico
variable "log_retention_days" {
  description = "Días para retener logs de acceso antes de borrar"
  type        = number
  default     = 30
}

# TODO: Para producción real, crear usuario IAM dedicado con estos servicios:
# - S3: lectura/escritura en buckets talentmap-*
# - DynamoDB: lectura/escritura en talentmap-tf-locks
# - Glue: CreateJob, StartJobRun, GetJobRun
# - SageMaker: CreateTrainingJob, CreateEndpoint
# - Lambda: CreateFunction, UpdateFunctionCode
# - IAM: PassRole (para roles de Glue y SageMaker)
# - CloudWatch: PutMetricData, CreateLogGroup
# - StepFunctions: CreateStateMachine, StartExecution
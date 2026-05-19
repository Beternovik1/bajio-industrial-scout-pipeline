# infrastructure/backend-setup/main.tf
# ============================================================================
# PROPÓSITO: Configurar el backend remoto de Terraform en AWS
# Este código crea la infraestructura para guardar el estado de Terraform de
# forma segura, con auditoría, versionamiento y locking distribuido.
# ============================================================================

terraform {
  # Versión mínima de Terraform requerida
  required_version = ">= 1.5.0"

  # Proveedores requeridos - especificamos AWS v5.x
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0" # ~> = v5.x (no v6.0+)
    }
  }
}

# Configuración del proveedor AWS
# region: región donde se crearán los recursos
# default_tags: etiquetas aplicadas automáticamente a TODOS los recursos
provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = "Hirenovik" 
      ManagedBy = "Terraform"
      Purpose   = "StateBackend"
    }
  }
}
# ============================================================================
# S3 Bucket para Logs de Acceso (SEPARADO del estado)
# ============================================================================
# POR QUÉ SEPARADO?
# - AWS NO permite que un bucket escriba logs sobre sí mismo (evita recursión)
# - Separando, los logs se acumulan en bucket distinto y se limpian automáticamente
# - Permite retención independiente: estado 90 días, logs 30 días
# - Mejora auditoría: logs no interfieren con el estado

resource "aws_s3_bucket" "terraform_state_logs" {
  bucket = "${var.state_bucket_name}-logs"

  lifecycle {
    prevent_destroy = true # Protección: imposible eliminar accidentalmente
  }

  tags = {
    Name = "Terraform State Logs"
  }
}

# Bloqueo de acceso público para el bucket de LOGS
# Protección adicional: imposible que los logs sean públicos
resource "aws_s3_bucket_public_access_block" "terraform_state_logs" {
  bucket = aws_s3_bucket.terraform_state_logs.id

  block_public_acls       = true # Bloquea ACLs públicas
  block_public_policy     = true # Bloquea políticas públicas
  ignore_public_acls      = true # Ignora ACLs públicas existentes
  restrict_public_buckets = true # Restringe acceso público general
}

# ============================================================================
# S3 Bucket para el Estado de Terraform (PRINCIPAL)
# ============================================================================
# Este archivo contiene la representación de tu infraestructura en AWS:
# - Qué recursos existen
# - Su configuración actual
# - Las dependencias entre ellos
# 
# CRÍTICO ASEGURAR:
# - Solo equipo autorizado pueda acceder
# - Sea imposible corruptarlo (versionamiento, cifrado)
# - Se guarden versiones para recuperación

resource "aws_s3_bucket" "terraform_state" {
  bucket = var.state_bucket_name

  lifecycle {
    prevent_destroy = true # Protección: imposible eliminar
  }

  tags = {
    Name = "Terraform State Storage"
  }
}

# VERSIONAMIENTO: Guarda cada cambio como versión separada
# Beneficios:
# - Rollback: Recuperar versión anterior si algo sale mal
# - Auditoría: Ver historial de cómo cambió la infraestructura
# - Recuperación: Protección contra corrupción accidental
resource "aws_s3_bucket_versioning" "enabled" {
  bucket = aws_s3_bucket.terraform_state.id

  versioning_configuration {
    status = "Enabled" # Cada cambio = nueva versión
  }
}

# ENCRIPTACIÓN: Protege datos sensibles en el estado
# El estado contiene información SENSIBLE:
# - Contraseñas de RDS
# - Claves de API
# - IDs de certificados SSL
# - Configuración de bases de datos
# NUNCA debe estar sin cifrado
# 
# AES256 = Encriptación server-side (AWS maneja claves)
resource "aws_s3_bucket_server_side_encryption_configuration" "default" {
  bucket = aws_s3_bucket.terraform_state.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256" # Cifra automáticamente
    }
    bucket_key_enabled = true # Optimización: reduce costos ~33%
  }
}

# BLOQUEO DE ACCESO PÚBLICO: CRÍTICO para seguridad
# Incluso si alguien crea una política pública por error, AWS bloqueará
# Esta es una "trampa de seguridad" adicional: NUNCA debe ser público
resource "aws_s3_bucket_public_access_block" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id

  block_public_acls       = true # Bloquea: grant-public-read, grant-public-write
  block_public_policy     = true # Bloquea: políticas que otorguen acceso público
  ignore_public_acls      = true # Ignora: ACLs públicas que ya existan
  restrict_public_buckets = true # Restringe: acceso público a nivel de bloque
}

# POLÍTICA S3: Fuerza HTTPS para TODO
# Esta política DENIEGA cualquier operación que NO use HTTPS/TLS
# Beneficio: Tráfico del estado SIEMPRE encriptado en tránsito
# 
# Cómo funciona:
# - DENIEGA si: aws:SecureTransport = false (no usa TLS)
# - Aplica a: todos (público o autorizados)
# - Effect = Deny = más fuerte que Allow (sin excepciones)
resource "aws_s3_bucket_policy" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "EnforcedTLS"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource = [
          aws_s3_bucket.terraform_state.arn,
          "${aws_s3_bucket.terraform_state.arn}/*"
        ]
        Condition = {
          Bool = {
            "aws:SecureTransport" = "false"
          }
        }
      }
    ]
  })

  depends_on = [aws_s3_bucket_public_access_block.terraform_state]
}

# LOGGING: Auditoría de accesos al estado
# Cada acceso (read/write) genera un log en el bucket de logs
# Beneficios:
# - Auditoría: Quién accedió, cuándo, qué operación
# - Debugging: Si falla algo, tienes registros
# - Compliance: Cumple regulaciones (HIPAA, PCI-DSS, SOC2)
# 
# target_bucket = bucket separado (no el mismo = evita recursión)
# target_prefix = carpeta dentro del bucket
resource "aws_s3_bucket_logging" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id

  target_bucket = aws_s3_bucket.terraform_state_logs.id
  target_prefix = "terraform-state-logs/"
}

# LIFECYCLE: Limpieza automática de versiones antiguas
# Las versiones antiguas ocupan espacio (y cuestan dinero)
# Esta regla limpia automáticamente versiones que ya no se necesitan
# 
# noncurrent_days = 90: Mantén 90 días de histórico
# Después de 90 días, borra versiones antiguas automáticamente
resource "aws_s3_bucket_lifecycle_configuration" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id

  rule {
    id     = "expire-old-versions"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = var.version_retention_days
    }
  }
}

# LIFECYCLE: Limpieza automática de logs
# Los logs se acumulan rápidamente y generan costos
# Esta regla borra automáticamente logs después de 30 días
# 
# Por qué 30 días?
# - Suficiente para debugging e investigación
# - Cumple regulaciones
# - Reduce costos de almacenamiento significativamente
resource "aws_s3_bucket_lifecycle_configuration" "terraform_state_logs" {
  bucket = aws_s3_bucket.terraform_state_logs.id

  rule {
    id     = "expire-logs"
    status = "Enabled"

    filter {
      prefix = "terraform-state-logs/"
    }

    expiration {
      days = var.log_retention_days
    }
  }
}

# ============================================================================
# DynamoDB para State Locking
# ============================================================================
# Problema sin locking:
# - Si 2 personas ejecutan terraform apply simultáneamente
# - Ambas actualizan el estado
# - Estado se CORROMPE (conflicto)
#
# Solución: Un "lock" en DynamoDB
# - El primero que ejecuta terraform apply "toma" el lock
# - El segundo espera a que termine el primero
# - Solo uno puede modificar el estado al mismo tiempo

resource "aws_dynamodb_table" "terraform_locks" {
  name         = var.lock_table_name
  billing_mode = "PAY_PER_REQUEST" # Paga por lo que uses (no reservas)
  hash_key     = "LockID"          # Clave primaria: guarda el lock_id

  # Define el atributo de la clave primaria
  attribute {
    name = "LockID" # Nombre del atributo
    type = "S"      # Tipo de dato: String
  }

  # Backup automático: recuperación ante desastres
  point_in_time_recovery {
    enabled = true # Costo: ~$0.20/mes (muy barato para seguridad)
  }

  tags = {
    Name = "Terraform State Locks"
  }
}

# ============================================================================
# Outputs: Valores que otros módulos/código necesitará
# ============================================================================
# Estos outputs exportan información de los recursos creados aquí
# para que otros módulos o archivos terraform.tfvars puedan utilizarlos

output "state_bucket_id" {
  description = "ID del bucket S3 para el estado de Terraform"
  value       = aws_s3_bucket.terraform_state.id
}

output "state_bucket_arn" {
  description = "ARN del bucket S3 (Amazon Resource Name)"
  value       = aws_s3_bucket.terraform_state.arn
}

output "dynamodb_table_name" {
  description = "Nombre de la tabla DynamoDB para locks"
  value       = aws_dynamodb_table.terraform_locks.name
}

output "dynamodb_table_arn" {
  description = "ARN de la tabla DynamoDB (para políticas IAM)"
  value       = aws_dynamodb_table.terraform_locks.arn
}

output "log_bucket_id" {
  description = "ID del bucket S3 para logs de acceso"
  value       = aws_s3_bucket.terraform_state_logs.id
}

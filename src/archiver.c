#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <libgen.h>

#include <getopt.h>

static struct option g_oLongOptions[] =
{
  {"new", required_argument, 0, 'n'},
  {"dir", required_argument, 0, 'd'},
  {"add", required_argument, 0, 'a'},
  {0, 0, 0, 0}
};

typedef enum A_ERROR
{

  GE_NO_ERROR,
  GE_NULL_POINTER_ERROR,
  GE_IO_ERROR,
  GE_UNABLE_TO_CREATE_ARCHIVE,
  GE_UNABLE_TO_OPEN_ARCHIVE,
  GE_UNABLE_TO_OPEN_INPUT,
  GE_MAGIC_VALUE_CORRUPT,
  GE_ARCHIVE_INDEX_CORRUPT,
  GE_UNEXPECTED_WRITE_ERROR,
  GE_UNEXPECTED_EOF

} A_ERROR;

/// Maximale Anzahl von Zeichen für einen Dateinamen
#define MAX_FILENAME  256

/// Indexeinträge pro Block
#define NUM_INDICES   16

/// Magic Value
#define MAGIC_VALUE   0x4242


#ifndef min
#define min(a,b) (((a) < (b)) ? (a) : (b))
#endif


/// Magic Value
typedef uint16_t MAGIC;


// Status eines Indexeintrages
typedef enum STATUS {

  /// freier Eintrag
  GS_FREE,

  /// Eintrag belegt
  GS_ACTUAL,

  /// Eintrag wurde gelöscht
  GS_DELETED,

  /// Eintrag wurde überschrieben
  GS_REPLACED,

  /// Letzter Eintrag im Archiv
  GS_EOA,

  /// Letzter Eintrag im Block
  GS_CONTINUE

} STATUS;


/// User ID
typedef uint32_t UID;


/// Group ID
typedef uint32_t GID;


//Type für die Indexeinträge
typedef enum TYPE {

  GT_DIRECTORY,

  GT_FILE

} TYPE;


/// type that stores a filename with a static number of chars
typedef char FILENAME[MAX_FILENAME];


/// unsigned integer type that stores the size of a file
typedef size_t SIZE;


/// unsigned integer type that stores the position of a file
typedef size_t POSITION;


// Type für die Indexeinträge
typedef struct INDEX {

  /// Status
  STATUS    m_eStatus     __attribute__ ((packed));

  /// User ID
  UID       m_uUserId     __attribute__ ((packed));

  /// Group ID
  GID       m_uGroupId    __attribute__ ((packed));

  /// Type des Eintrags
  TYPE      m_eType       __attribute__ ((packed));

  /// Dateiname
  FILENAME  m_csFilename;

  /// Länge
  SIZE      m_szLength    __attribute__ ((packed));

  /// Position
  POSITION  m_szPosition  __attribute__ ((packed));

} INDEX, *PINDEX;


/// Statische Nummer der Indexeinträge
typedef INDEX INDICES[NUM_INDICES];


//Type für einen Indizeblock
typedef struct BLOCK
{

  MAGIC     m_uMagic      __attribute__ ((packed));

  INDICES   m_ssIndices;

} BLOCK, *PBLOCK;

/**
 * Liest einen Indizeblock und speichert diesen an der Stelle von pIndexBlock
 *
 * \param fdArchiveFile
 *
 * \param pIndexBlock
 *
 * \return
 *
 */
static A_ERROR readBlock(int fdArchiveFile, PBLOCK pIndexBlock)
{
  A_ERROR eResult;    // return
  ssize_t szOverallRead;
  ssize_t szActualRead;
  char *pPosition;

  if (pIndexBlock != NULL)
  {
    pPosition = (char*) pIndexBlock;  // Anfang
    szOverallRead = 0;                // Reset counter

    // Reset memory
    memset(pIndexBlock, 0, sizeof(BLOCK));

    while ((szActualRead = read(fdArchiveFile, pPosition, sizeof(BLOCK)-szOverallRead)) > 0)
    {
      szOverallRead += szActualRead;
      pPosition += szActualRead;
    }

    // Wenn alles eingelesen wurde oder EOF erreicht wurde
    if (szActualRead == -1)
      eResult = GE_IO_ERROR;
    else
    {

      //EOF vor dem Ende des aktuellem Blocks
      if (szOverallRead != sizeof(BLOCK))
        eResult = GE_UNEXPECTED_EOF;
      else if (pIndexBlock->m_uMagic != MAGIC_VALUE) // Magic number wird validiert
        eResult = GE_MAGIC_VALUE_CORRUPT;
      else
        eResult = GE_NO_ERROR;

    }

  } else
  {
    eResult = GE_NULL_POINTER_ERROR;
  }

  return eResult;
}


/**
 *
 * Schreibt einen Indizeblock
 *
 * \param fdArchiveFile
 *
 * \param pIndexBlock
 *
 * \return
 *
 */
static A_ERROR write_block(int fdArchiveFile, PBLOCK pIndexBlock)
{
  A_ERROR eResult;
  ssize_t szOverallWritten;
  ssize_t szActualWritten;
  char *pPosition;

  if (pIndexBlock != NULL)
  {
    pPosition = (char*) pIndexBlock;  // Anfang der Daten
    szOverallWritten = 0;             // Reset Counter
    pIndexBlock->m_uMagic = MAGIC_VALUE;

    // Schreiben
    while ((szActualWritten = write(fdArchiveFile, pPosition, sizeof(BLOCK)-szOverallWritten)) > 0)
    {
      // Counter für Bytes die bereits geschrieben wurden
      szOverallWritten += szActualWritten;
      pPosition += szActualWritten;
    }

    if (szActualWritten == -1)
      eResult = GE_IO_ERROR;
    else // szActualWritten == 0 -> Alles wurde geschrieben
    {

      // Flush data
      if (fsync(fdArchiveFile) == -1)
        eResult = GE_IO_ERROR;
      else
        eResult = GE_NO_ERROR;

    }

  } else
  {
    eResult = GE_NULL_POINTER_ERROR;
  }

  return eResult;
}


/**
 * Eine vorgegebene Anzahl von Bytes wird aus dem Inputfile in das Outputfile kopert.
 *
 * \param fdOutputFile
 *
 * \param fdInputFile
 *
 * \param szNumBytes
 *
 * \param szBlkSize
 *
 * \return
 *
 */
static A_ERROR copy_data(int fdOutputFile, int fdInputFile, size_t szNumBytes, size_t szBlkSize)
{
  ssize_t szRead;
  size_t szBlkRead;
  ssize_t szWrite;
  size_t szBlkWrite;
  A_ERROR eResult = GE_NO_ERROR;
  char *pcBuffer = (char*) alloca(szBlkSize);
  char *pcPosition;

  do
  {

    // Block wird gelesen
    pcPosition = pcBuffer;
    szBlkRead  = 0;
    while ((szRead = read(fdInputFile, pcPosition, min(szBlkSize-szBlkRead, szNumBytes))) > 0)
    {
      szBlkRead  += szRead;
      pcPosition += szRead;
    }

    if (szRead < 0)
      eResult = GE_IO_ERROR;
    else
    {

      // Block wird geschrieben
      pcPosition = pcBuffer;
      szBlkWrite = 0;
      while ((szWrite = write(fdOutputFile, pcPosition, szBlkRead-szBlkWrite)) > 0)
      {
        szNumBytes -= szWrite;
        szBlkWrite += szWrite;
        pcPosition += szWrite;
      }

      if (szWrite < 0)
        eResult = GE_IO_ERROR;

    }

  } while (!eResult && (szNumBytes > 0));

  return eResult;
}

/**
 * Neues Archiv wird erstellt
 *
 * \param pcArchiveName
 *
 * \return
 *
 */
A_ERROR new(const char *pcArchiveName)
{
  A_ERROR eResult;
  BLOCK sBlock;
  int fdArchiveFile = creat(pcArchiveName, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);

  if (fdArchiveFile != -1)
  {
    // Blockdaten mit 0 initialisieren
    memset(&sBlock, 0, sizeof(BLOCK));

    // Statusfeld = letzter Indexeintrag
    sBlock.m_ssIndices[NUM_INDICES - 1].m_eStatus = GS_EOA;

    // Archivfile wird geschrieben
    eResult = write_block(fdArchiveFile, &sBlock);

    if (!eResult)
      printf("new archive '%s' created.\n", pcArchiveName);

    close(fdArchiveFile);

  } else
  {
    eResult = GE_UNABLE_TO_CREATE_ARCHIVE;
  }

  return eResult;
}


/**
 * Listet den Inhalt des Archivfiles auf
 *
 * \param pcArchiveName
 *
 * \return
 *
 */
A_ERROR dir(const char *pcArchiveName)
{
  A_ERROR eResult;
  BLOCK sBlock;
  PINDEX pIndex;
  size_t uIndexId;
  int bEndOfArchive;
  int fdArchiveFile = open(pcArchiveName, O_RDONLY);

  if (fdArchiveFile != -1)
  {
    printf("Einträge von '%s'\n", pcArchiveName);
    printf("-----------------------\n\n");
    do
    {
      eResult = readBlock(fdArchiveFile, &sBlock);
      if (!eResult)
      {
          printf("%s %13s %8s %8s %12s %8s\n", "Size", "Filename", "UID", "GID", "Status", "Type");
        for (uIndexId = 0; uIndexId < NUM_INDICES; uIndexId++)
        {
          pIndex = &(sBlock.m_ssIndices[uIndexId]);
          printf("Index (%zu) Status: %d\n", uIndexId, pIndex->m_eStatus);

          switch (pIndex->m_eStatus)
          {

          case GS_ACTUAL:
        	  printf("%zu %12s %13u %8u %6lu %10lu\n", pIndex->m_szLength, pIndex->m_csFilename, pIndex->m_uUserId, pIndex->m_uGroupId, pIndex->m_eStatus, pIndex->m_eType);
            break;

          default:
            break;

          }

        }

        switch (sBlock.m_ssIndices[NUM_INDICES - 1].m_eStatus)
        {
        case GS_CONTINUE:
          if (lseek(fdArchiveFile, sBlock.m_ssIndices[NUM_INDICES - 1].m_szPosition, SEEK_CUR) == -1)
            eResult = GE_IO_ERROR;
          else
            bEndOfArchive = 0;
          break;

        case GS_EOA:
          bEndOfArchive = 1;
          break;

        default:
          eResult = GE_ARCHIVE_INDEX_CORRUPT;
          break;
        }

      }
    } while (!eResult && !bEndOfArchive);

    close(fdArchiveFile);

  } else
  {
    eResult = GE_UNABLE_TO_OPEN_ARCHIVE;
  }
  return eResult;
}

/**
 * Fügt die Datei in ein Archiv ein
 *
 * \param pcArchiveName
 *
 * \param pcInputName
 *
 * \return
 *
 */
A_ERROR add(const char *pcArchiveName, const char *pcInputName)
{
  A_ERROR eResult;
  BLOCK sBlock;
  struct stat sStat;
  char *pcCopyInputName = (char*) alloca(strlen(pcInputName) + 1);
  char *pcFilename;
  size_t uIndexId;
  size_t uBlockPos;
  size_t uNextData;
  int bEndOfArchive;
  int bFoundFreeBlock;
  int fdArchiveFile = open(pcArchiveName, O_RDWR);
  int fdInputFile = open(pcInputName, O_RDONLY);

  // Filename herausfinden
  strcat(pcCopyInputName, pcInputName);
  pcFilename = basename(pcCopyInputName);

  if (fdArchiveFile != -1)
  {
    if (fdInputFile != -1)
    {
      //Finde den ersten freien Eintrag
      uNextData = sizeof(BLOCK); //Offset
      bFoundFreeBlock = 0;
      do
      {
        uBlockPos = lseek(fdArchiveFile, 0, SEEK_CUR);
        if (uBlockPos == -1)
          eResult = GE_IO_ERROR;
        else
          eResult = readBlock(fdArchiveFile, &sBlock);

        if (!eResult)
        {

          for (uIndexId = 0; (uIndexId < NUM_INDICES) && !bFoundFreeBlock; uIndexId = bFoundFreeBlock ? uIndexId : uIndexId + 1)
          {
            if (sBlock.m_ssIndices[uIndexId].m_eStatus == GS_FREE)
              bFoundFreeBlock = 1;
            else if (sBlock.m_ssIndices[uIndexId].m_eStatus != GS_CONTINUE)
              uNextData = sBlock.m_ssIndices[uIndexId].m_szPosition + sBlock.m_ssIndices[uIndexId].m_szLength;
          }

          if (!bFoundFreeBlock)
          {

            switch (sBlock.m_ssIndices[NUM_INDICES - 1].m_eStatus)
            {
            case GS_CONTINUE:
              // Position wird an den Beginn des nächsten Blocks gesetzt
              if (lseek(fdArchiveFile, sBlock.m_ssIndices[NUM_INDICES - 1].m_szPosition, SEEK_CUR) == -1)
                eResult = GE_IO_ERROR;
              else
                bEndOfArchive = 0;
              break;

            case GS_EOA:
              bEndOfArchive = 1;
              break;

            default:
              eResult = GE_ARCHIVE_INDEX_CORRUPT;
              break;
            }

          }

        }
      } while (!eResult && !bEndOfArchive && !bFoundFreeBlock);


      if (!eResult)
      {
        if (bFoundFreeBlock)
        {
          printf("found free block at 0x%zx\n", uNextData);

          if (lseek(fdArchiveFile, (ssize_t) uNextData, SEEK_SET) == -1)
            eResult = GE_IO_ERROR;
          else
          {

            // Länge des Inputfiles
            memset(&sStat, 0, sizeof(struct stat));
            if (fstat(fdInputFile, &sStat) == -1)
              eResult = GE_IO_ERROR;
            else
            {
              printf("'%s' filesize is %zd, filename ist '%s'\n", pcInputName, sStat.st_size, pcFilename);

              eResult = copy_data(fdArchiveFile, fdInputFile, sStat.st_size, sStat.st_blksize);
              if (!eResult)
              {
                if (lseek(fdArchiveFile, (ssize_t) uBlockPos, SEEK_SET) == -1)
                  eResult = GE_IO_ERROR;
                else
                {
                  // Block updaten
                  sBlock.m_ssIndices[uIndexId].m_eStatus    = GS_ACTUAL;
                  sBlock.m_ssIndices[uIndexId].m_uUserId    = sStat.st_uid;
                  sBlock.m_ssIndices[uIndexId].m_uGroupId   = sStat.st_gid;
                  sBlock.m_ssIndices[uIndexId].m_eType      = S_ISDIR(sStat.st_mode) ? GT_DIRECTORY : GT_FILE;
                  strncat(sBlock.m_ssIndices[uIndexId].m_csFilename, pcFilename, MAX_FILENAME);
                  sBlock.m_ssIndices[uIndexId].m_szLength   = sStat.st_size;
                  sBlock.m_ssIndices[uIndexId].m_szPosition = uNextData;

                  eResult = write_block(fdArchiveFile, &sBlock);
                }

              }

            }

          }


        } else
        {
          eResult = GE_ARCHIVE_INDEX_CORRUPT;
        }
      }
      close(fdInputFile);
    } else
    {
      eResult = GE_UNABLE_TO_OPEN_INPUT;
    }

    close(fdArchiveFile);

  }
  {
    eResult = GE_UNABLE_TO_OPEN_ARCHIVE;
  }
  return eResult;
}

int main(int argc, char *argv[])
{
  int iParameter;
  int iOptionIndex;

  // getopt example http://www.gnu.org/software/libc/manual/html_node/Getopt-Long-Option-Example.html#Getopt-Long-Option-Example

  iOptionIndex = 0;
  iParameter = getopt_long(argc, argv, "n:d:a:", g_oLongOptions, &iOptionIndex);

  if (iParameter != -1)
  {
    switch (iParameter)
    {

    case 'n':
      new(optarg);
      break;

    case 'd':
      dir(optarg);
      break;

    case 'a':
      while (optind < argc)
        add(optarg, argv[optind++]);
      break;

    case '?':
      break;

    default:
      abort ();
    }
  }

  return 0;
}
